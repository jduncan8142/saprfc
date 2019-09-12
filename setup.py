import setuptools
import io
import os
import sys

required = ['readconfig', 'redis', 'pandas', 'pyrfc']

here = os.path.abspath(os.path.dirname(__file__))

with open("purrfc/README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="purrfc",
    version="0.0.2",
    author="Jason Duncan",
    author_email="jason.matthew.duncan@gmail.com",
    description="Wrapper for SAP RFC connector PyRFC",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jduncan8142/saprfc",
    install_requires=required,
    include_package_data=True,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    package_data={'purrfc': ['requirements.txt', 'README.md', 'LICENSE']},
    py_modules=[
        'purrfc'
    ],
)
