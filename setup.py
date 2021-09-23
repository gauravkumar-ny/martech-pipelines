from setuptools import find_packages, setup
from martech_pipelines import __version__

setup(
    name="martech_pipelines",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    version=__version__,
    description="Martech Pipelines",
    author="gaurav.kumar@nykaa.com",
)
