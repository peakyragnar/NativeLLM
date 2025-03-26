#!/usr/bin/env python3
"""
SEC iXBRL Document Renderer

This module implements rendering of SEC iXBRL documents using Arelle,
the same open-source tool used by the SEC for their viewer.
"""

import os
import sys
import logging
import subprocess
import tempfile
import time
import shutil
from pathlib import Path

# Constants
ARELLE_DOWNLOAD_URL = "https://github.com/Arelle/Arelle/releases/latest/download/arelle-win-x64.zip"
ARELLE_SCRIPT_URL = "https://raw.githubusercontent.com/Arelle/Arelle/master/scripts/runArelleCmdLine.py"

class ArelleRenderer:
    """
    SEC iXBRL document renderer using Arelle.
    
    This class provides methods to render iXBRL documents using Arelle,
    the same open-source tool used by the SEC for their iXBRL viewer.
    """
    
    def __init__(self, arelle_path=None, temp_dir=None, 
                 install_if_missing=True, validate_install=True):
        """
        Initialize the Arelle-based renderer.
        
        Args:
            arelle_path: Path to Arelle installation or script
            temp_dir: Directory for temporary files
            install_if_missing: Whether to install Arelle if not found
            validate_install: Whether to validate Arelle installation
        """
        # Set up paths
        self.arelle_path = arelle_path
        
        # Use system temp directory if none provided
        if temp_dir:
            self.temp_dir = Path(temp_dir)
            os.makedirs(self.temp_dir, exist_ok=True)
        else:
            self.temp_dir = Path(tempfile.gettempdir()) / "ixbrl_renderer"
            os.makedirs(self.temp_dir, exist_ok=True)
        
        # Validate or find Arelle installation
        if not self.arelle_path:
            self.arelle_path = self._find_arelle()
        
        # Install Arelle if missing and requested
        if not self.arelle_path and install_if_missing:
            self.arelle_path = self._install_arelle()
        
        # Validate installation if requested
        if validate_install and self.arelle_path:
            if not self._validate_arelle():
                logging.error("Arelle installation validation failed")
                self.arelle_path = None
        
        if not self.arelle_path:
            logging.warning("Arelle not found or installation failed")
            logging.warning("You will need to install Arelle manually to use this renderer")
            logging.warning("Visit: https://arelle.org/arelle/")
        else:
            logging.info(f"Arelle renderer initialized with path: {self.arelle_path}")
    
    def _find_arelle(self):
        """
        Find Arelle installation on the system.
        
        Returns:
            Path to Arelle executable or None if not found
        """
        # Look for Arelle in common locations
        possible_paths = [
            # Standard installation locations
            "/usr/local/bin/arelle",
            "/usr/bin/arelle",
            "/opt/arelle/arelleCmdLine.py",
            
            # Windows installation locations
            "C:/Program Files/Arelle/arelleCmdLine.exe",
            "C:/Arelle/arelleCmdLine.exe",
            
            # macOS installation locations
            "/Applications/Arelle.app/Contents/MacOS/arelleCmdLine",
            
            # Python package installations
            shutil.which("arelle"),
            shutil.which("arelleCmdLine"),
            shutil.which("python -m arelle.CntlrCmdLine")
        ]
        
        # Check if any of these paths exist
        for path in possible_paths:
            if path and os.path.exists(path):
                logging.info(f"Found Arelle at: {path}")
                return path
        
        # Not found
        logging.warning("Arelle not found in standard locations")
        return None
    
    def _install_arelle(self):
        """
        Install Arelle if missing.
        
        Returns:
            Path to installed Arelle or None if installation failed
        """
        logging.info("Attempting to install Arelle...")
        
        # Create installation directory
        install_dir = self.temp_dir / "arelle_install"
        os.makedirs(install_dir, exist_ok=True)
        
        # For now, install Arelle via pip (simpler cross-platform approach)
        try:
            logging.info("Installing Arelle via pip...")
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "arelle"],
                check=True
            )
            
            # Try to find the installed package
            arelle_path = shutil.which("arelle")
            if not arelle_path:
                # Try to find the module
                try:
                    import arelle
                    arelle_path = os.path.join(os.path.dirname(arelle.__file__), "CntlrCmdLine.py")
                    logging.info(f"Found Arelle module at: {arelle_path}")
                except ImportError:
                    logging.error("Arelle package installed but module not found")
                    return None
            
            logging.info(f"Arelle installed successfully at: {arelle_path}")
            return arelle_path
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to install Arelle via pip: {str(e)}")
            
            # Fall back to downloading the script directly
            try:
                logging.info(f"Downloading Arelle script from: {ARELLE_SCRIPT_URL}")
                
                # Download the script
                import requests
                response = requests.get(ARELLE_SCRIPT_URL)
                if response.status_code == 200:
                    script_path = install_dir / "runArelleCmdLine.py"
                    with open(script_path, 'w') as f:
                        f.write(response.text)
                    
                    # Make executable
                    os.chmod(script_path, 0o755)
                    
                    logging.info(f"Downloaded Arelle script to: {script_path}")
                    return str(script_path)
                else:
                    logging.error(f"Failed to download Arelle script: HTTP {response.status_code}")
                    return None
            
            except Exception as e:
                logging.error(f"Failed to download Arelle script: {str(e)}")
                return None
    
    def _validate_arelle(self):
        """
        Validate Arelle installation by running a simple command.
        
        Returns:
            True if validation succeeded, False otherwise
        """
        if not self.arelle_path:
            return False
        
        logging.info("Validating Arelle installation...")
        
        try:
            # Determine how to run Arelle - first try as a module if installed via pip
            try:
                cmd = [sys.executable, "-m", "arelle.CntlrCmdLine", "--help"]
                logging.info(f"Trying to validate Arelle as a Python module")
                result = subprocess.run(
                    cmd,
                    check=False,
                    capture_output=True,
                    text=True
                )
                
                if "arelle" in result.stdout.lower() or "xbrl" in result.stdout.lower() or "usage" in result.stdout.lower():
                    logging.info("Arelle module validation successful")
                    self.arelle_path = "module"  # Mark as module-based
                    return True
            except Exception as module_e:
                logging.warning(f"Failed to validate Arelle as module: {str(module_e)}")
            
            # If module approach failed, try direct path
            if self.arelle_path.endswith(".py"):
                # Python script
                cmd = [sys.executable, self.arelle_path, "--help"]
            else:
                # Executable
                cmd = [self.arelle_path, "--help"]
            
            # Run basic help command
            result = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True
            )
            
            # Check if output contains expected Arelle help text
            if "arelle" in result.stdout.lower() or "xbrl" in result.stdout.lower():
                logging.info("Arelle validation successful")
                return True
            else:
                logging.warning(f"Arelle validation failed: Unexpected output")
                logging.debug(f"Output: {result.stdout}")
                return False
                
        except Exception as e:
            logging.error(f"Arelle validation failed: {str(e)}")
            return False
    
    def render_ixbrl(self, input_file, output_format="html", output_file=None):
        """
        Render an iXBRL file using Arelle.
        
        Args:
            input_file: Path to iXBRL file
            output_format: Output format (html, xhtml, xml, json)
            output_file: Output file path (if None, generates a temporary file)
            
        Returns:
            Path to rendered output file
        """
        if not self.arelle_path:
            raise ValueError("Arelle not found or not properly installed")
        
        if not os.path.exists(input_file):
            raise ValueError(f"Input file not found: {input_file}")
        
        # Set default output file if not provided
        if not output_file:
            if output_format == "html":
                ext = ".html"
            elif output_format == "xml":
                ext = ".xml"
            elif output_format == "json":
                ext = ".json"
            else:
                ext = ".xhtml"
            
            # Create temp file
            output_file = self.temp_dir / f"rendered_{int(time.time())}{ext}"
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        
        # Determine how to run Arelle
        if self.arelle_path == "module":
            # Run as Python module
            cmd_base = [sys.executable, "-m", "arelle.CntlrCmdLine"]
        elif self.arelle_path.endswith(".py"):
            # Python script
            cmd_base = [sys.executable, self.arelle_path]
        else:
            # Executable
            cmd_base = [self.arelle_path]
        
        # Build command based on output format
        if output_format == "html":
            # HTML output (most common format for human-readable rendering)
            cmd = cmd_base + [
                "--file", input_file,
                "--plugins", "transforms/SEC",  # SEC transformation rules
                "--save-instance", output_file
            ]
        elif output_format == "xml":
            # XML output (XBRL format)
            cmd = cmd_base + [
                "--file", input_file,
                "--save-instance", output_file
            ]
        elif output_format == "json":
            # JSON output
            cmd = cmd_base + [
                "--file", input_file,
                "--plugins", "xbrlJson",
                "--save-json", output_file
            ]
        else:
            # Default to XHTML
            cmd = cmd_base + [
                "--file", input_file,
                "--save-instance", output_file
            ]
        
        # Run Arelle
        logging.info(f"Rendering iXBRL file: {input_file}")
        logging.info(f"Command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True
            )
            
            # Check for errors
            if result.returncode != 0:
                logging.error(f"Arelle rendering failed with code {result.returncode}")
                logging.error(f"Error output: {result.stderr}")
                raise Exception(f"Arelle rendering failed: {result.stderr}")
            
            # Check if output file was created
            if not os.path.exists(output_file):
                logging.error(f"Output file not created: {output_file}")
                logging.error(f"Arelle output: {result.stdout}")
                raise Exception(f"Output file not created: {output_file}")
            
            logging.info(f"Successfully rendered iXBRL to: {output_file}")
            return output_file
            
        except Exception as e:
            logging.error(f"Error rendering iXBRL file: {str(e)}")
            raise Exception(f"Error rendering iXBRL file: {str(e)}")

    def extract_text(self, ixbrl_file, output_file=None):
        """
        Extract text from an iXBRL file using Arelle.
        
        This is a convenience method that:
        1. Renders the iXBRL file to HTML
        2. Extracts plain text from the HTML
        
        Args:
            ixbrl_file: Path to iXBRL file
            output_file: Output text file path (if None, returns text string)
            
        Returns:
            Extracted text as string or path to output file
        """
        # First render to HTML
        html_file = self.render_ixbrl(ixbrl_file, output_format="html")
        
        # Extract text from HTML
        try:
            from bs4 import BeautifulSoup
            
            # Parse HTML
            with open(html_file, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()
            
            # Get text
            text = soup.get_text()
            
            # Clean up text
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            # Save to file if requested
            if output_file:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(text)
                logging.info(f"Extracted text saved to: {output_file}")
                return output_file
            else:
                return text
                
        except Exception as e:
            logging.error(f"Error extracting text from HTML: {str(e)}")
            raise Exception(f"Error extracting text from HTML: {str(e)}")
    
    def cleanup(self):
        """
        Clean up temporary files.
        """
        try:
            # Remove temporary directory and contents
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                logging.info(f"Removed temporary directory: {self.temp_dir}")
                
        except Exception as e:
            logging.error(f"Error cleaning up temporary files: {str(e)}")


# Example usage
if __name__ == "__main__":
    import argparse
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Render SEC iXBRL documents using Arelle")
    parser.add_argument("input_file", help="Path to iXBRL file to render")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--format", choices=["html", "xml", "json", "text"], default="html",
                      help="Output format (default: html)")
    parser.add_argument("--arelle-path", help="Path to Arelle executable or script")
    
    args = parser.parse_args()
    
    # Create renderer
    renderer = ArelleRenderer(arelle_path=args.arelle_path)
    
    # Render file
    if args.format == "text":
        # Extract text
        result = renderer.extract_text(args.input_file, args.output)
        if not args.output:
            print("\nExtracted Text:\n")
            print(result[:1000] + "...[truncated]" if len(result) > 1000 else result)
    else:
        # Render to specified format
        result = renderer.render_ixbrl(args.input_file, args.format, args.output)
    
    print(f"\nRendering complete: {result}")
    
    # Clean up
    renderer.cleanup()