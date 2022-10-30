import concurrent
import urllib
from dataclasses import dataclass, InitVar

import cv2
import numpy as np
import time
from base64 import b64decode, b64encode

# from matplotlib import pyplot as plt
from operator import itemgetter
from typing import List, Tuple, Any, Callable, Optional

from mashumaro import DataClassJSONMixin, DataClassDictMixin

def sort_contours(cnts, method="left-to-right"):
    # https://github.com/jacksonrya/cv-box-detection/blob/master/src/box_detection.py
    # initialize the reverse flag and sort index
    reverse = False
    i = 0

    # handle if we need to sort in reverse
    if method == "right-to-left" or method == "bottom-to-top":
        reverse = True

    # handle if we are sorting against the y-coordinate rather than
    # the x-coordinate of the bounding box
    if method == "top-to-bottom" or method == "bottom-to-top":
        i = 1

    # construct the list of bounding boxes and sort them from top to
    # bottom
    boundingBoxes = [cv2.boundingRect(c) for c in cnts]
    (cnts, boundingBoxes) = zip(*sorted(zip(cnts, boundingBoxes),
                                        key=lambda b: b[1][i], reverse=reverse))

    # return the list of sorted contours and bounding boxes
    return (cnts, boundingBoxes)


@dataclass()
class Rectangle(DataClassJSONMixin, DataClassDictMixin):
    X1: float
    X2: float
    Y1: float
    Y2: float

    @property
    def width(self) -> float:
        return self.X2-self.X1

    @property
    def height(self) -> float:
        return self.Y2-self.Y1

    @staticmethod
    def get_image_height(image: np.ndarray) -> int:
        return image.shape[0]

    @staticmethod
    def get_image_width(image: np.ndarray) -> int:
        return image.shape[1]

    def get_bounding_box(self, image: np.ndarray) -> np.ndarray:
        y1 = round(self.get_image_height(image) * self.Y1)
        y2 = round(self.get_image_height(image) * self.Y2)
        x1 = round(self.get_image_width(image) * self.X1)
        x2 = round(self.get_image_width(image) * self.X2)

        return image[y1:y2, x1:x2]


class ScaleType():
    NONE = "NONE" # Do not scale the image
    SCALE_SOURCE = "SCALE_SOURCE" # scale the source image to match template size
    # SCALE_TEMPLATE = "SCALE_TEMPLATE" # Currently not supported

@dataclass
class ImageScale(DataClassJSONMixin, DataClassDictMixin):
    min_scale: float = 0.5
    max_scale: float = 1.0
    scale_steps: int = 5
    scale_type: str = ScaleType.SCALE_SOURCE


@dataclass()
class Image(DataClassJSONMixin, DataClassDictMixin):
    name: str
    path: Optional[str] = None
    bytes: InitVar[str] = None
    image_np: InitVar[np.ndarray] = None
    imread: int = cv2.IMREAD_GRAYSCALE
    prefetch: bool = True

    def __post_init__(self, bytes: str, image_np: np.ndarray):
        arg_cnt = 0
        if self.path is not None:
            arg_cnt += 1

        if bytes is not None:
            arg_cnt += 1

        if image_np is not None:
            arg_cnt += 1

        if arg_cnt > 1:
            raise ValueError("Path, Bytes and image_np can't be set at the same time")

        # if arg_cnt == 0:
        #     raise ValueError("Either path, bytes or image_np must have value")

        self._image = None
        self._image_encoded = None

        if image_np is not None:
            self._image = image_np
        elif bytes is not None:
            self._image = self.decode_image(bytes)
        elif self.path is not None and self.prefetch:
            self._read_image()


    def _read_image(self):
        if self.path is not None:
            try:
                req = urllib.request.urlopen(self.path)
                arr = np.asarray(bytearray(req.read()), dtype=np.uint8)
                self._image = cv2.imdecode(arr, self.imread)  # 'Load it as it is'
            except ValueError:
                self._image = None

            if self._image is None: #still none, so let's try a local path
                try:
                    self._image = cv2.imread(self.path, self.imread)
                except:
                    self._image = None

    @property
    def image(self):
        if self._image is None:
            self._read_image()
        return self._image

    @property
    def image_encoded(self):
        if self._image_encoded is None:
            self._image_encoded = self.encode_image(self.image)
        return self._image_encoded

    def encode_image(self, image: np.ndarray) -> str:
        _, compressed_image = cv2.imencode(".jpg", image)
        return b64encode(compressed_image.tobytes()).decode("ascii")

    def decode_image(self, content: str) -> np.ndarray:
        if content is None:
            return None

        arr: np.ndarray = np.frombuffer(b64decode(content), dtype=np.uint8)
        return cv2.imdecode(arr, self.imread)

    def update_image(self, new_image: np.ndarray):
        self._image = new_image

    @staticmethod
    def crop(image: np.ndarray, dim: Rectangle):
        cropped = image[int(dim.Y1):int(dim.Y2), int(dim.X1):int(dim.X2)]
        return cropped

    @staticmethod
    def resize(image: np.ndarray, scale):
        img = image

        # resize the image according to the scale, and keep track
        # of the ratio of the resizing

        width = int(img.shape[1] * scale)
        height = int(img.shape[0] * scale)
        dim = (width, height)

        resized = cv2.resize(img, dim)
        # resized = imutils.resize(img, width=int(img.shape[1] * scale))
        # ratio = img.shape[1] / float(resized.shape[1])

        return resized

    # def write_image(self, write_func: Callable):
    #     return write_func(self)
    #
    # def read_image(self, read_func: Callable):
    #     self._image = read_func(self)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


@dataclass()
class ImageTemplate(Image):
    dim: Optional[Rectangle] = None
    absolute_dim: Optional[Rectangle] = None # the bounding box of the match relativ to the parent image
    confidence: Optional[float] = None
    scaling_factor: Optional[float] = None

    def __post_init__(self, bytes: str, image_np: np.ndarray):
        self.imread = cv2.IMREAD_COLOR
        super(ImageTemplate, self).__post_init__(bytes, image_np)



@dataclass()
class TemplateMatch(Image):
    persist: bool = False # should a match be persisted?
    dim: Rectangle = None # the actual size of the matched area
    absolute_dim: Optional[Rectangle] = None # the bounding box of the match relative to the parent image
    src_img_name: str = None # source image
    src_template_name: str = None # source template
    confidence: float = None
    scaling_factor: float = None
    type: Optional[str] = "" # basically the name of the found result

@dataclass()
class PatternMatchOperation(DataClassJSONMixin, DataClassDictMixin):
    # image: Optional[Image] = None
    template: Optional[ImageTemplate] = None
    match: Optional[TemplateMatch] = None
    children: Optional[List['PatternMatchOperation']] = None
    do_plot: Optional[bool] = False
    persist: bool = False  # should a match be persisted?
    type: Optional[str] = ""
    scale: Optional[ImageScale] = None # should the image be scaled before matching?
    alg: int = cv2.TM_CCOEFF_NORMED # used templating algo
    margin_width: Optional[float] = 0.0
    margin_height : Optional[float] = 0.0

    def __post_init__(self):
        self._parent: PatternMatchOperation = None
        self._root: PatternMatchOperation = None

    def match_template(self, image: np.ndarray = None) -> Tuple[float, float]:
        # match Template
        # temp_img = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        temp_img = image
        result = cv2.matchTemplate(temp_img, self.template.image, self.alg)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)

        # If the method is TM_SQDIFF or TM_SQDIFF_NORMED, take minimum
        if self.alg in (cv2.TM_SQDIFF, cv2.TM_SQDIFF_NORMED):
            loc = min_loc
        else:
            loc = max_loc

        return max_val, loc

    def canny_inner(self, image : np.ndarray, scale, tH, tW) -> Tuple[float, float, float, np.ndarray]:
        if scale != 1:
            # resized, ratio, scale = image.resize(scale)
            resized_image = Image.resize(image, scale)
            # resized_image = image.resize(scale)
        else:
            # resized, ratio, scale = (image.image, 1, 1) # optimization to direct access instead of resize method
            resized_image = image

        # if the resized image is smaller than the template, then break
        # from the loop
        if resized_image.shape[0] < tH or resized_image.shape[1] < tW:
            return None

        confidence, x1position = self.match_template(resized_image)

        return confidence, x1position, scale, resized_image

    def canny(self, image: Image) -> Tuple[float, Rectangle, float, np.ndarray]:
        results = []
        best_result = None

        if self.scale is None or self.scale.scale_type == ScaleType.NONE:
            scale_min = 1
            scale_max = 1
            scale_steps = 1
        else: # we try multiple sizes of the template, because we don't know it's exact size
            scale_min = self.scale.min_scale
            scale_max = self.scale.max_scale
            scale_steps = self.scale.scale_steps

        # loop over the scales of the image
        iteration = 1
        (tH, tW) = self.template.image.shape[:2]

        for scale in np.linspace(scale_min, scale_max, scale_steps)[::-1]:
            # res = (confidence, x1_position, scale, resized_image)
            temp_result = self.canny_inner(image.image, scale, tH, tW)

            if temp_result is not None and (best_result is None or temp_result[0] > best_result[0]):
                best_result = temp_result

            # if res is None:
            #     continue
            #
            # results.append(res)

            # print(
            #     f"Iteration: {iteration}, Scale: {scale}, Correlation: {res[0]}, Time_elapsed: {time.time() - loop_start}")
            iteration += 1

        # (confidence, x1_position, scaling_factor, resized) = max(results, key=itemgetter(0))
        if not best_result:
            return None, None, None, None

        (confidence, x1_position, scale, resized_image) = best_result

        # TODO

        # # calculate bounding box
        # (startX, startY) = (int(maxLoc[0] * r), int(maxLoc[1] * r))
        # (endX, endY) = (int((maxLoc[0] + tW) * r), int((maxLoc[1] + tH) * r))
        #
        # # calculate bounding box of resized image
        # (startX, startY) = (int(maxLoc[0]), int(maxLoc[1]))
        # (endX, endY) = (int((maxLoc[0] + tW)), int((maxLoc[1] + tH)))

        # The coordinates of the detected template on the resized source image
        dim = Rectangle(
            X1=int(max(x1_position[0] - self.margin_width, 0)),
            Y1=int(max(x1_position[1] - self.margin_height, 0)),
            X2=int((x1_position[0] + tW + self.margin_width)),
            Y2=int((x1_position[1] + tH + self.margin_height))
        )

        cropped_image = Image.crop(resized_image, dim)

        # return resized, dim, max_val, ratio
        return confidence, dim, scale, cropped_image

        # return cropped_image_by_template, (startX, startY), (endX, endY), maxVal, r


    # def plot(self, src, title):
    #     return # safety switch
    #     if self.do_plot:
    #         plt.imshow(src)
    #         plt.title(title), plt.xticks([]), plt.yticks([])
    #         plt.show()

    # def find_template_in_image(self, image: Image):
    #     # process template
    #     src, dimensions, max_val, factor = self.canny(image)
    #
    #     self.template.dim = dimensions
    #     self.template.confidence = max_val
    #     self.template.scaling_factor = factor
    #
    #     if self.children is None:
    #         return
    #
    #     for node in self.children:
    #         node.find_template_in_image(src)
    #
    # def generate_matches(self, image: Image):
    #     cropped = image.crop(self.template.dim)
    #
    #     self.match = TemplateMatch(f"{image.name}__{self.template.name}__",
    #                                image_np=cropped,
    #                                persist=self.persist,
    #                                src_img_name=image.name,
    #                                src_template_name=self.template.name,
    #                                dim=self.template.dim,
    #                                confidence=self.template.confidence,
    #                                scaling_factor=self.template.scaling_factor,
    #                                type=self.type
    #                                )
    #
    #     if self.children is None:
    #         return self.match
    #
    #     for node in self.children:
    #         node.find_template_in_image(src)
    #
    #     self.match.absolute_dim = self.get_absolute_bounding_box()

    def execute_match_pattern(self, image: Image) -> TemplateMatch:
        # process template
        # src, dimensions, max_val, factor = self.canny(image)
        confidence, dim, scale, cropped_image = self.canny(image)

        if not confidence or not dim or not scale or cropped_image is None:
            return None

        # image.update_image(resized_image)
        # cropped = image.crop(dim)

        # TODO
        match = TemplateMatch(f"{image.name}__{self.template.name}__",
                                   image_np=cropped_image,
                                   persist=self.persist,
                                   src_img_name=image.name,
                                   src_template_name=self.template.name,
                                   dim=dim,
                                   confidence=confidence,
                                   scaling_factor=scale,
                                   type=self.type
                                   )

        return match


    def get_absolute_bounding_box(self, src_dim: Rectangle = None):
        # take dimensions of the found area
        # place dimensions relative to source image (actually the first match contains exactly this)
        if src_dim is None:
            return self.match.dim

        bounding_box = Rectangle(X1=0, X2=0, Y1=0, Y2=0)

        bounding_box.X1 = (src_dim.X1 + self.match.dim.X1) * self.match.scaling_factor
        bounding_box.X2 = (bounding_box.X1 + self.match.dim.width) * self.match.scaling_factor
        bounding_box.Y1 = (src_dim.Y1 + self.match.dim.Y1) * self.match.scaling_factor
        bounding_box.Y2 = (bounding_box.Y1 + self.match.dim.height) * self.match.scaling_factor

        return bounding_box

    def get_absolute_dimensions(self, src_image: Image, bounding_box: Rectangle = None, width: float = None, height: float = None) -> Rectangle:
        if bounding_box is None:
            bounding_box = Rectangle(X1=0, X2=0, Y1=0, Y2=0)
            # bounding_box.X1 = self.match.dim.X1 * self.match.scaling_factor
            # bounding_box.X2 = self.match.dim.X2 * self.match.scaling_factor
            # bounding_box.Y1 = self.match.dim.Y1 * self.match.scaling_factor
            # bounding_box.Y2 = self.match.dim.Y2 * self.match.scaling_factor

        bounding_box.X1 = (bounding_box.X1 + self.match.dim.X1) * self.match.scaling_factor
        bounding_box.X2 = (bounding_box.X1 + self.match.dim.width) * self.match.scaling_factor
        bounding_box.Y1 = (bounding_box.Y1 + self.match.dim.Y1) * self.match.scaling_factor
        bounding_box.Y2 = (bounding_box.Y1 + self.match.dim.height) * self.match.scaling_factor

        if self._root is not None and self._root != self:
            return self._parent.get_absolute_dimensions(bounding_box, self.match.dim.width, self.match.dim.height)
        else:
            # reached top element
            # get relative position to source image
            # relative coordinates
            (rH, rW) = src_image.image.shape[:2] # Todo
            relative_dim = Rectangle(
                X1=bounding_box.X1/rW,
                X2=bounding_box.X2/rW,
                Y1=bounding_box.Y1/rH,
                Y2=bounding_box.Y2/rH
            )

            return relative_dim

    def execute_bounding_box_calculation_chain(self, absolute_dim: Rectangle = None):
        try:
            if not self.match:
                return

            self.match.absolute_dim = self.get_absolute_bounding_box(absolute_dim)

            if self.children is not None:
                for node in self.children:
                    node.execute_bounding_box_calculation_chain(self.match.absolute_dim)
                    # self.execute_bounding_box_calculation_chain(self.match.absolute_dim)
                    # node.match.absolute_dim = node.get_absolute_bounding_box(self.match.absolute_dim)
        except Exception as e:
            self.log.debug(e)


    def execute_pattern_matching_chain(self, image : Image, parent=None, root=None, matches=None) -> TemplateMatch:
        if matches is None:
            matches = []

        if image is None:
            return matches

        if parent is None:
            self._parent = self
            self._root = self
        else:
            self._parent = parent
            self._root = root

        self.match = self.execute_match_pattern(image)

        if self.match:
            matches.append(self.match)

        if self.children is not None:
            for node in self.children:
                node.execute_pattern_matching_chain(
                    self.match,
                    parent=self._parent,
                    root=self._root,
                    matches=matches
                )


        # can be performed later?
        # self.match.absolute_dim = self.get_absolute_dimensions(src_image=image)

        return matches

    # OBSOLETE
    def apply_template_map(self, image: Image):
        # this method can be executed after the template engine has run at least once.
        # it assumes that matches have been found and the matches can now be applied to a different image
        # it's not really elegant, but it does the trick
        self.image = image

        resized = self.image.resize(1 / self.match.scaling_factor)[0]
        cropped = resized[self.match.dim.Y1:self.match.dim.Y2, self.match.dim.X1:self.match.dim.X2]

        self.match = TemplateMatch(f"{self.image.name}__{self.template.name}__",
                                   image_np=cropped,
                                   persist=self.template.persist,
                                   src_img_name=self.image.name,
                                   src_template_name=self.template.name,
                                   dim=self.match.dim,
                                   confidence=self.match.confidence,
                                   scaling_factor=self.match.scaling_factor,
                                   type=self.type
                                   )

        self.match.absolute_dim = self.get_absolute_dimensions()

        if self.children is not None:
            for node in self.children:
                node.apply_template_map(self.match)

    def __iter__(self):
        yield self

        if self.children is not None:
            for child in self.children:
                for node in child:
                    yield node



@dataclass()
class PatternMatchChain(DataClassJSONMixin, DataClassDictMixin):
    template_map: List[PatternMatchOperation]

    def match_templates(self, image: Image):
        # execute the map on any given image and return the positions
        for node in self.template_map:
            node.find_template_in_image(image)
        pass

    def generate_image_slices(self, image):
        # just use the fund positions and crop an image accordingly into pieces
        pass


# get grayscale image
def get_grayscale(image):
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)


# noise removal
def remove_noise(image):
    return cv2.medianBlur(image, 5)


# thresholding
def thresholding(image):
    return cv2.threshold(image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]


# dilation
def dilate(image, value=5):
    kernel = np.ones((value, value), np.uint8)
    return cv2.dilate(image, kernel, iterations=1)


# erosion
def erode(image, value=5, iter=1):
    kernel = np.ones((value, value), np.uint8)
    return cv2.erode(image, kernel, iterations=iter)


# opening - erosion followed by dilation
def opening(image):
    kernel = np.ones((5, 5), np.uint8)
    return cv2.morphologyEx(image, cv2.MORPH_OPEN, kernel)


# canny edge detection
def canny(image):
    return cv2.Canny(image, 100, 200)
