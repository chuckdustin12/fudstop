import inspect
from master import master_sdk



# Populate the available functions dictionary
available_functions = {}
for func in master_sdk.MasterSDK().functions:
    available_functions[func.__name__] = {"parameters": inspect.signature(func).parameters}

# Construct the schema
tools = []
for func_name, func_info in available_functions.items():
    func_schema = {
        "type": "function",
        "function": {
            "name": func_name,
            "description": "",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    }
    for param_name, param in func_info["parameters"].items():
        param_type = str(param.annotation).replace("<class '", "").replace("'>", "")
        if param.default == inspect.Parameter.empty:
            func_schema["function"]["parameters"]["required"].append(param_name)
        func_schema["function"]["parameters"]["properties"][param_name] = {
            "type": param_type,
            "description": ""
        }
    tools.append(func_schema)

# The final code snippet
print(tools)

def print_class_functions_with_args(cls):
    for func_name in dir(cls):
        if callable(getattr(cls, func_name)) and not func_name.startswith("__"):
            func = getattr(cls, func_name)
            args = inspect.signature(func)
            print(f"{func_name}{args}")