from distutils.errors import CompileError
from subprocess import call

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
import os
import platform
import subprocess

class BuildGoExt(build_ext):
    """Custom command to build extension from Go source files"""
    def build_extension(self, ext):
        ext_path = self.get_ext_fullpath(ext.name)
        cmd = ['go', 'build', '-buildmode=c-shared', '-o', ext_path]
        cmd += ext.sources
        out = call(cmd)
        if out != 0:
            raise CompileError('Go build failed')
        
# Print out our uname for debugging purposes
uname = platform.uname()
print(uname)

# Install OSX Golang if needed
if uname.system == "Darwin":
    subprocess.call(["sh", "./build_scripts/setup-macos.sh"])

# Install Linux Golang if needed
elif uname.system == "Linux":
    if uname.machine == "aarch64":
        subprocess.call(["sh", "./build_scripts/setup-arm64.sh"])
    elif uname.machine in ("armv7l", "armv6l"):
        subprocess.call(["sh", "./build_scripts/setup-arm6vl.sh"])
    elif uname.machine == "x86_64":
        subprocess.call(["sh", "./build_scripts/setup-linux-64.sh"])
    elif uname.machine == "i686":
        subprocess.call(["sh", "./build_scripts/setup-linux-32.sh"])

# Add in our downloaded Go compiler to PATH
old_path = os.environ["PATH"]
new_path = os.path.join(os.getcwd(), "go", "bin")
env = {"PATH": f"{old_path}:{new_path}"}
env = dict(os.environ, **env)
os.environ["PATH"] = f"{old_path}:{new_path}"

with open("README.md", "r") as rd:
    long_description: str = rd.read()

setup(
    name='sql-server-lineage',
    version='1.0.0',
    description='Python library to extract lineage from Sql Server stored procedures',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Ivan Ostymchuk',
    python_requires=">=3.8",
    py_modules=['sql_server_lineage'],
    ext_modules=[
        Extension('_sql_server_lineage', ['sql_server_lineage_export.go'])
    ],
    cmdclass={'build_ext': BuildGoExt},
    zip_safe=False,
    license='MIT',
)