from dataclasses import dataclass
from typing import Optional

class DTO():

    def deepcopy(self, obj=None, source=None, exclude: () = None):

        if obj is None:
            return None

        # define the direction of the copy
        if source is None:  # by default copy from obj to dto
            src = obj
            dst = self
        elif source == self:  # explicit copy from dto to obj
            src = self
            dst = obj
        elif source == obj:  # explicit copy from obj to dto
            src = obj
            dst = self
        else:  # error case
            return None

        # for m in members :
        for key, value in src.__dict__.items():
            if key.startswith("__") or key.startswith("_") or callable(key):
                continue
            if exclude is not None and key in exclude:
                continue
            setattr(dst, key, value)
        return dst

    # reference:
    # https://www.codegrepper.com/code-examples/whatever/python+nested+object+to+dict
    def to_dict(self, obj=None):
        if obj is None:
            obj = self
        if not hasattr(obj, "__dict__"):
            return obj
        result = {}
        for key, val in obj.__dict__.items():
            if key.startswith("_"):
                continue
            element = []
            if isinstance(val, list):
                for item in val:
                    element.append(self.to_dict(item))
            else:
                if val is not None:
                    element = self.to_dict(val)
                else:
                    element = None
            result[key] = element
        return result


@dataclass()
class FIFA2020DTO(DTO):
    home_score: int
    away_score: int
    score_format: str
    game_time: str
    game_time_seconds: int
    game_time_format: str
    home_team: Optional[str]
    away_team: Optional[str]
