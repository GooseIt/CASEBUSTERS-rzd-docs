import os

from Parsing.M11 import parse_M11
from Parsing.FMU76 import parse_FMU76

INPUT_DIR = "../Files_in"
OUTPUT_DIR = "../Dicts_in"


def main() -> None:
    for path_name in os.listdir(INPUT_DIR):
        mypath = f"{INPUT_DIR}/{path_name}"
        print(mypath)

        try:
            res = parse_M11(mypath)
        except:
            try:
                res = parse_FMU76(mypath)
            except:
                res = "Parsing failed"

        with open(f"{OUTPUT_DIR}/{path_name[:-4]}.txt", "w") as f:
            f.write(str(res))


if __name__ == "__main__":
    main()
