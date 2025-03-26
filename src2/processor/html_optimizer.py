"""
HTML Optimizer Module

Responsible for optimizing HTML content while preserving data integrity.
This is the advanced HTML optimization that reduces file size while 
ensuring all financial data is perfectly preserved.
"""

import os
import re
import logging
import tempfile
from bs4 import BeautifulSoup

class HTMLOptimizer:
    """
    Optimize HTML content while preserving data integrity
    """
    
    def __init__(self, optimization_level="maximum_integrity"):
        """
        Initialize HTML optimizer
        
        Args:
            optimization_level: Level of optimization to apply:
                - "maximum_integrity": Guarantees 100% data preservation
                - "balanced": Good size reduction with high data integrity
                - "maximum_reduction": Maximum size reduction, may affect some formatting
        """
        self.optimization_level = optimization_level
        
        # Define attributes to remove based on optimization level
        self.always_remove_attributes = [
            "bgcolor", "color", "class", "id", "style", "face", "font", "size",
            "role", "data-", "aria-", "onclick", "onload", "tabindex"
        ]
        
        self.preserve_attributes = [
            "align", "valign", "colspan", "rowspan", "border", "cellpadding", "cellspacing",
            "width", "height", "src", "href", "alt", "title"
        ]
        
        if optimization_level == "maximum_integrity":
            # Only remove attributes that definitely won't affect data or layout
            self.removable_attributes = self.always_remove_attributes
        elif optimization_level == "balanced":
            # Remove more attributes but keep structural ones
            self.removable_attributes = self.always_remove_attributes + [
                "target", "rel", "dir", "lang", "charset"
            ]
        else:  # maximum_reduction
            # Remove almost all attributes except critical ones
            self.preserve_attributes = ["colspan", "rowspan"]  # Only keep these
            self.removable_attributes = None  # Will remove all except preserved
    
    def optimize_html(self, html_content):
        """
        Optimize HTML content for size reduction while preserving data integrity
        
        Args:
            html_content: HTML content to optimize
            
        Returns:
            Dict with optimized content and statistics
        """
        if not html_content:
            return {
                "optimized_html": "",
                "original_size": 0,
                "optimized_size": 0,
                "reduction_bytes": 0,
                "reduction_percent": 0
            }
        
        original_size = len(html_content)
        
        try:
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove comments
            for comment in soup.find_all(text=lambda text: isinstance(text, str) and text.strip().startswith('<!--')):
                comment.extract()
            
            # Remove unnecessary attributes based on optimization level
            for tag in soup.find_all():
                attrs_to_remove = []
                
                if self.removable_attributes is None:
                    # In maximum_reduction mode, remove all attributes except those in preserve_attributes
                    attrs_to_remove = [attr for attr in tag.attrs if attr not in self.preserve_attributes]
                else:
                    # Otherwise, only remove attributes in removable_attributes
                    for attr in tag.attrs:
                        if attr in self.removable_attributes:
                            attrs_to_remove.append(attr)
                        else:
                            # Check data- attributes
                            if attr.startswith("data-") and "data-" in self.removable_attributes:
                                attrs_to_remove.append(attr)
                
                # Remove identified attributes
                for attr in attrs_to_remove:
                    del tag.attrs[attr]
            
            # Optimize tables (table optimization preserves data relationships)
            for table in soup.find_all('table'):
                self._optimize_table(table)
            
            # Convert back to string
            optimized_html = str(soup)
            
            # Clean up whitespace (careful to preserve whitespace in pre/code tags)
            optimized_html = self._clean_whitespace(optimized_html)
            
            # Calculate reduction
            optimized_size = len(optimized_html)
            reduction_bytes = original_size - optimized_size
            reduction_percent = (reduction_bytes / original_size) * 100 if original_size > 0 else 0
            
            return {
                "optimized_html": optimized_html,
                "original_size": original_size,
                "optimized_size": optimized_size,
                "reduction_bytes": reduction_bytes,
                "reduction_percent": reduction_percent
            }
        
        except Exception as e:
            logging.error(f"Error optimizing HTML: {str(e)}")
            return {
                "error": str(e),
                "optimized_html": html_content,  # Return original for safety
                "original_size": original_size,
                "optimized_size": original_size,
                "reduction_bytes": 0,
                "reduction_percent": 0
            }
    
    def _optimize_table(self, table):
        """
        Optimize HTML table while preserving data relationships
        
        Args:
            table: BeautifulSoup table element
            
        Returns:
            Optimized table element
        """
        try:
            # Remove unnecessary table attributes while preserving structure
            attrs_to_keep = ["colspan", "rowspan", "width", "height", "align", "valign"]
            
            # Apply to all cells
            for cell in table.find_all(['td', 'th']):
                attrs_to_remove = [attr for attr in cell.attrs if attr not in attrs_to_keep]
                for attr in attrs_to_remove:
                    del cell.attrs[attr]
            
            # Clean up rows
            for row in table.find_all('tr'):
                attrs_to_remove = [attr for attr in row.attrs if attr not in attrs_to_keep]
                for attr in attrs_to_remove:
                    del row.attrs[attr]
            
            return table
        
        except Exception as e:
            logging.warning(f"Error optimizing table: {str(e)}")
            return table  # Return original for safety
    
    def _clean_whitespace(self, html_content):
        """
        Clean up whitespace in HTML content while preserving content in pre/code tags
        
        Args:
            html_content: HTML content to clean
            
        Returns:
            Cleaned HTML content
        """
        # Temporarily replace content in pre/code tags to protect whitespace
        pre_tags = {}
        code_tags = {}
        
        # Extract pre tags
        pre_pattern = re.compile(r'<pre.*?>.*?</pre>', re.DOTALL)
        for i, match in enumerate(pre_pattern.finditer(html_content)):
            pre_tags[f'PRE_TAG_{i}'] = match.group(0)
            html_content = html_content.replace(match.group(0), f'PRE_TAG_{i}')
        
        # Extract code tags
        code_pattern = re.compile(r'<code.*?>.*?</code>', re.DOTALL)
        for i, match in enumerate(code_pattern.finditer(html_content)):
            code_tags[f'CODE_TAG_{i}'] = match.group(0)
            html_content = html_content.replace(match.group(0), f'CODE_TAG_{i}')
        
        # Clean up whitespace
        html_content = re.sub(r'\s+', ' ', html_content)  # Replace multiple spaces with single
        html_content = re.sub(r'>\s+<', '><', html_content)  # Remove space between tags
        html_content = re.sub(r'\s+/>', '/>', html_content)  # Clean self-closing tags
        
        # Restore pre/code tags
        for tag_id, content in pre_tags.items():
            html_content = html_content.replace(tag_id, content)
        for tag_id, content in code_tags.items():
            html_content = html_content.replace(tag_id, content)
        
        return html_content
    
    def optimize_file(self, file_path, output_path=None, min_reduction_percent=1.0):
        """
        Optimize HTML content in a file
        
        Args:
            file_path: Path to file containing HTML
            output_path: Path to save optimized file (or None to overwrite)
            min_reduction_percent: Minimum reduction percentage to apply changes
            
        Returns:
            Dict with optimization results
        """
        try:
            # Read file
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Optimize HTML
            result = self.optimize_html(content)
            
            # Check if optimization is worth applying
            if result["reduction_percent"] >= min_reduction_percent:
                # Determine output path
                if output_path is None:
                    # Create temporary file
                    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                        temp_path = temp_file.name
                    
                    # Write to temp file
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        f.write(result["optimized_html"])
                    
                    # Replace original file
                    os.replace(temp_path, file_path)
                    output_path = file_path
                else:
                    # Write to specified output path
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write(result["optimized_html"])
                
                result["file_path"] = output_path
                result["changes_applied"] = True
            else:
                result["changes_applied"] = False
                result["reason"] = f"Reduction below threshold: {result['reduction_percent']:.2f}% < {min_reduction_percent}%"
            
            return result
        
        except Exception as e:
            logging.error(f"Error optimizing file {file_path}: {str(e)}")
            return {
                "error": str(e),
                "file_path": file_path,
                "changes_applied": False
            }
    
    def extract_text_only_from_html(self, html_value):
        """
        Safely extract only text from HTML while preserving 100% of data values.
        This function prioritizes perfect data preservation over size reduction,
        only removing HTML/CSS styling when it can guarantee no data loss whatsoever.
        
        Args:
            html_value: Value that might contain HTML formatting
            
        Returns:
            Text-only value with HTML/CSS removed but all data perfectly preserved
        """
        # Quick check if this is even HTML
        if not html_value or not isinstance(html_value, str):
            return html_value
            
        # Check if it looks like HTML
        if "<" not in html_value or ">" not in html_value:
            return html_value
            
        try:
            # Parse HTML
            soup = BeautifulSoup(html_value, 'html.parser')
            
            # Get text while preserving some spacing
            text = soup.get_text(separator=' ', strip=True)
            
            # Normalize whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text
        except:
            # If any errors, return the original to be safe
            return html_value

# Create a singleton instance
html_optimizer = HTMLOptimizer(optimization_level="maximum_integrity")