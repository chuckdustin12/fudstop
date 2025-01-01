import pandas as pd
import os
import platform
import mimetypes
import time
import ast
from datetime import datetime

class FileSDK:
    def __init__(self):
        pass

    def get_file_details(self, file_path):
        if not os.path.exists(file_path):
            return f"Error: The file path '{file_path}' does not exist."

        file_details = {}

        # Basic details
        file_details['File Path'] = os.path.abspath(file_path)
        file_details['Exists'] = os.path.exists(file_path)
        file_details['Is File'] = os.path.isfile(file_path)
        file_details['Is Directory'] = os.path.isdir(file_path)

        # Path details
        file_details['Parent Directory'] = os.path.dirname(os.path.abspath(file_path))
        file_details['File Name'] = os.path.basename(file_path)
        file_details['File Extension'] = os.path.splitext(file_path)[1]

        # File size
        if os.path.isfile(file_path):
            file_details['Size (Bytes)'] = os.path.getsize(file_path)
            file_details['Size (KB)'] = round(os.path.getsize(file_path) / 1024, 2)
            file_details['Size (MB)'] = round(os.path.getsize(file_path) / (1024 * 1024), 2)

        # Timestamps
        file_details['Created Time'] = datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
        file_details['Last Modified Time'] = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
        file_details['Last Accessed Time'] = datetime.fromtimestamp(os.path.getatime(file_path)).strftime('%Y-%m-%d %H:%M:%S')

        # MIME type
        mime_type, encoding = mimetypes.guess_type(file_path)
        file_details['MIME Type'] = mime_type or 'Unknown'
        file_details['Encoding'] = encoding or 'Unknown'

        # Permissions
        file_details['Readable'] = os.access(file_path, os.R_OK)
        file_details['Writable'] = os.access(file_path, os.W_OK)
        file_details['Executable'] = os.access(file_path, os.X_OK)

        # System information
        file_details['System OS'] = platform.system()
        file_details['OS Version'] = platform.version()
        file_details['OS Release'] = platform.release()

        return file_details

    def display_file_details(self, file_details):
        if isinstance(file_details, str):  # Error message
            print(file_details)
        else:
            print("\nFile Details:")
            print("----------------")
            for key, value in file_details.items():
                print(f"{key}: {value}")


    def parse_python_file(self, file_path):
        if not os.path.exists(file_path):
            return {"error": f"The file '{file_path}' does not exist."}
        
        with open(file_path, 'r') as file:
            source_code = file.read()
        
        tree = ast.parse(source_code)
        details = {
            "imports": [],
            "functions": [],
            "classes": [],
        }

        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    details["imports"].append({"module": alias.name, "as": alias.asname})
            elif isinstance(node, ast.ImportFrom):
                details["imports"].append({
                    "module": node.module,
                    "names": [alias.name for alias in node.names],
                    "level": node.level,
                })
            elif isinstance(node, ast.FunctionDef):
                details["functions"].append({
                    "name": node.name,
                    "args": [arg.arg for arg in node.args.args],
                    "docstring": ast.get_docstring(node),
                    "decorators": [d.id for d in node.decorator_list if isinstance(d, ast.Name)],
                })
            elif isinstance(node, ast.ClassDef):
                class_info = {
                    "name": node.name,
                    "docstring": ast.get_docstring(node),
                    "methods": [],
                }
                for subnode in node.body:
                    if isinstance(subnode, ast.FunctionDef):
                        class_info["methods"].append({
                            "name": subnode.name,
                            "args": [arg.arg for arg in subnode.args.args],
                            "docstring": ast.get_docstring(subnode),
                        })
                details["classes"].append(class_info)

        return details