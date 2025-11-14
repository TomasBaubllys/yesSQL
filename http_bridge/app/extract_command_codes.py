import re
from pathlib import Path 

INPUT_PATH = Path("../server/include/commands.h")
OUTPUT_PATH = Path("command_codes.py")

enum_name = "Command_Code"
commands = {}

in_enum = False
current_value = 0

for line in INPUT_PATH.read_text().splitlines():
    line = line.strip()
    if line.startswith(f"typedef enum {enum_name}"):
        in_enum = True
        continue
    if in_enum:
        if line.startswith("}"):
            break

        line = line.rstrip(",")

        if not line:
            continue

        if line.startswith("//"):
            continue

        if "=" in line:
            name, val = map(str.strip, line.split("="))
            current_value = int(val)
        else:
            name = line
        
        commands[name] = current_value
        current_value += 1


with OUTPUT_PATH.open("w") as f:
    for name, val in commands.items():
        f.write(f"{name} = {val}\n")


