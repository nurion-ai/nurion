"""
Custom build hooks for fusionflowkit package.
Handles JAR file preparation during build process.
"""

import glob
import os
import subprocess
import sys
from shutil import copy2
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

JARS_TARGET = os.path.join("raydp", "jars")


class BuildWithJars(_build_py):
    """Custom build_py command that handles JAR files."""

    def run(self):
        # Setup JAR files before building
        self.setup_jars()

        # Run the normal build
        super().run()

    def setup_jars(self):
        """Set up JAR files for packaging."""
        CORE_DIR = os.path.abspath("java")

        # Build JAR files using Maven
        self.build_jars(CORE_DIR)

        JARS_PATH = glob.glob(
            os.path.join(CORE_DIR, "**/target/raydp-*.jar"), recursive=True
        ) + glob.glob(os.path.join(CORE_DIR, "thirdparty/*.jar"))

        if len(JARS_PATH) == 0:
            print(
                "Can't find core module jars after Maven build. Build may have failed.",
                file=sys.stderr,
            )
            raise RuntimeError("JAR files not found after Maven build")

        # Clean up existing temp directory if it exists
        if os.path.exists(JARS_TARGET):
            # Remove only JAR files, not the entire directory
            if os.path.exists(JARS_TARGET):
                for jar_file in glob.glob(os.path.join(JARS_TARGET, "*.jar")):
                    try:
                        os.remove(jar_file)
                        print(f"Removed existing JAR file: {jar_file}")
                    except OSError as e:
                        print(f"Failed to remove {jar_file}: {e}", file=sys.stderr)

        try:
            os.makedirs(JARS_TARGET, exist_ok=True)
        except Exception as e:
            print(f"Failed to create temp directories: {e}", file=sys.stderr)
            raise

        try:
            for jar_path in JARS_PATH:
                print(f"Copying {jar_path} to {JARS_TARGET}")
                copy2(jar_path, JARS_TARGET)
            print(f"Successfully copied {len(JARS_PATH)} JAR files")
        except Exception as e:
            print(f"Failed to copy JAR files: {e}", file=sys.stderr)
            raise

    def build_jars(self, core_dir):
        """Build JAR files using Maven."""
        # Check if Maven is available
        try:
            subprocess.run(["mvn", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Maven (mvn) could not be found. Please install Maven first.", file=sys.stderr)
            raise RuntimeError("Maven not found")

        print(f"Building JAR files in {core_dir}")

        # Save current directory
        original_dir = os.getcwd()

        try:
            # Change to core directory and run Maven build
            os.chdir(core_dir)
            print("Running: mvn clean package -DskipTests")

            subprocess.run(
                ["mvn", "clean", "package", "-DskipTests"],
                check=True,
                capture_output=False,  # Let Maven output be visible
            )

            print("Maven build completed successfully")

        except subprocess.CalledProcessError as e:
            print(f"Maven build failed with exit code {e.returncode}", file=sys.stderr)
            raise RuntimeError(f"Maven build failed: {e}")
        except Exception as e:
            print(f"Failed to run Maven build: {e}", file=sys.stderr)
            raise
        finally:
            # Always restore original directory
            os.chdir(original_dir)


class SdistWithJars(_sdist):
    """Custom sdist command that handles JAR files."""

    def run(self):
        # Setup JAR files before creating source distribution
        build_cmd = BuildWithJars(self.distribution)
        build_cmd.setup_jars()

        # Run the normal sdist
        super().run()
