import custom_text_anonymizer
import importlib
import inspect

# Check the core module
try:
    core_module = importlib.import_module('text_anonymizer.core')
    print("✓ Loaded core module")
    print(f"Core module file: {inspect.getfile(core_module)}")
    
    # List functions/classes in core
    print("\nFunctions/classes in core module:")
    for name in dir(core_module):
        if not name.startswith('_'):
            print(f"  - {name}")
except ImportError as e:
    print(f"✗ Could not load core module: {e}")

# Check the main module too
try:
    main_module = importlib.import_module('text_anonymizer.main')
    print(f"\nMain module file: {inspect.getfile(main_module)}")
    
    print("\nFunctions/classes in main module:")
    for name in dir(main_module):
        if not name.startswith('_'):
            print(f"  - {name}")
except ImportError as e:
    print(f"✗ Could not load main module: {e}")


import os

# Get package directory
package_dir = os.path.dirname(inspect.getfile(custom_text_anonymizer))
print(f"Package directory: {package_dir}")

# Read the main __init__.py
init_file = os.path.join(package_dir, '__init__.py')
print(f"\n{'='*60}")
print(f"Contents of {init_file}:")
print('='*60)

with open(init_file, 'r') as f:
    print(f.read())

# Read the core module
core_file = os.path.join(package_dir, 'core.py')
if os.path.exists(core_file):
    print(f"\n{'='*60}")
    print(f"First 50 lines of core.py:")
    print('='*60)
    
    with open(core_file, 'r') as f:
        for i, line in enumerate(f):
            if i < 50:
                print(f"{i+1:3d}: {line.rstrip()}")
            else:
                print("... (truncated)")
                break
else:
    print(f"\ncore.py not found at {core_file}")