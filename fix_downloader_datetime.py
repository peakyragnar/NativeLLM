#!/usr/bin/env python3
"""
Fix datetime import in SEC downloader
"""

import re

# Find files that might need the datetime import
files_to_check = [
    "src2/sec/downloader.py",
    "src2/downloader/direct_edgar_downloader.py"
]

# Check and fix each file
for file_path in files_to_check:
    try:
        with open(file_path, "r") as f:
            content = f.read()
            
        # Check if datetime is imported
        if "import datetime" not in content and "from datetime import" not in content:
            # Add datetime import at the top of the imports section
            if "import os" in content:
                content = re.sub(
                    r'import os',
                    'import os\nimport datetime',
                    content
                )
            else:
                # Just add it at the top if there's no os import
                content = "import datetime\n" + content
                
            # Write the corrected file
            with open(file_path, "w") as f:
                f.write(content)
                
            print(f"✅ Added datetime import to {file_path}")
        else:
            print(f"✓ {file_path} already has datetime import")
    except FileNotFoundError:
        print(f"⚠️ File not found: {file_path}")
    except Exception as e:
        print(f"❌ Error processing {file_path}: {str(e)}")