import yaml
import inspect
import os

def get_caller_file_name():
    call_stack = inspect.stack()
    call_filenames = [stack.filename for stack in call_stack]
    common_file_name = "StockDocPython"
    filtered_filenames = [filename for filename in call_filenames if filename.endswith(".py") and common_file_name in filename]
    if len(filtered_filenames)==0:
        return "Not Found"
    caller_filename = os.path.basename(filtered_filenames[-1])
    return caller_filename

source_file_name = get_caller_file_name()
print(source_file_name)


if source_file_name == "OAuth2Security.py":
    filePath = "../params.yaml"
elif source_file_name == "ExtractProperty.py":
    filePath = "../../params.yaml"
elif source_file_name == "main.py":
    filePath = "params.yaml"
else:
    filePath = "params.yaml"


class Property:
    def __init__(self):
        with open(filePath, "r") as yamlfile:
            self.data = yaml.load(yamlfile, Loader=yaml.FullLoader)

    def get_property_data(self):
        return self.data

if __name__ == "__main__":
    # properties = Property()
    # print(properties.get_property_data())
    pass