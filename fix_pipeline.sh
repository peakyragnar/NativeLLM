#\!/bin/bash

# This script patches the pipeline.py file to fix the error handling for LLM formatting failures

# Save original file as backup
cp /Users/michael/NativeLLM/src2/sec/pipeline.py /Users/michael/NativeLLM/src2/sec/pipeline.py.bak

# Replace the first occurrence of the pattern
sed -i.tmp '
/# Don'"'"'t fail the pipeline if LLM formatting fails/{
n
n
n
c\
                error_msg = llm_result.get('"'"'error'"'"', '"'"'Unknown error'"'"')\
                logging.warning(f"LLM formatting failed: {error_msg}")\
                \
                # If the error is related to the '"'"'list'"'"' object has no attribute '"'"'items'"'"', \
                # it indicates an issue with the XBRL data format\
                if "'"'"'list'"'"' object has no attribute '"'"'items'"'"'" in str(error_msg):\
                    # This is a known issue that we'"'"'ve fixed, but we should mark it as an error to prevent \
                    # adding Firestore entries without corresponding GCS files\
                    result["error"] = f"LLM formatting failed with XBRL format error: {error_msg}"\
                    # This will prevent the pipeline from marking success when there'"'"'s no LLM file\
                    logging.error(f"Critical error: LLM formatting failed with XBRL format error. This will prevent metadata-only entries in Firestore.")\
                else:\
                    # For other LLM formatting errors, we'"'"'ll add a warning but continue processing\
                    # This allows text-only formats to still be processed\
                    result["warning"] = f"LLM formatting failed: {error_msg}"
}
' /Users/michael/NativeLLM/src2/sec/pipeline.py

# Replace the second occurrence of the pattern (if it exists)
sed -i.tmp2 '
/# Don'"'"'t fail the pipeline if LLM formatting fails/{
n
n
n
c\
                error_msg = llm_result.get('"'"'error'"'"', '"'"'Unknown error'"'"')\
                logging.warning(f"LLM formatting failed: {error_msg}")\
                \
                # If the error is related to the '"'"'list'"'"' object has no attribute '"'"'items'"'"', \
                # it indicates an issue with the XBRL data format\
                if "'"'"'list'"'"' object has no attribute '"'"'items'"'"'" in str(error_msg):\
                    # This is a known issue that we'"'"'ve fixed, but we should mark it as an error to prevent \
                    # adding Firestore entries without corresponding GCS files\
                    result["error"] = f"LLM formatting failed with XBRL format error: {error_msg}"\
                    # This will prevent the pipeline from marking success when there'"'"'s no LLM file\
                    logging.error(f"Critical error: LLM formatting failed with XBRL format error. This will prevent metadata-only entries in Firestore.")\
                else:\
                    # For other LLM formatting errors, we'"'"'ll add a warning but continue processing\
                    # This allows text-only formats to still be processed\
                    result["warning"] = f"LLM formatting failed: {error_msg}"
}
' /Users/michael/NativeLLM/src2/sec/pipeline.py

# Remove temporary files
rm /Users/michael/NativeLLM/src2/sec/pipeline.py.tmp*

echo "Pipeline.py has been patched to fix LLM formatting error handling"
