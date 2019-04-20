#%%
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bluesquare_data_pipelines",
    version="0.0.1",
    author="Grégoire Lurton",
    author_email="glurton@bluesquarehub.com",
    description="Package for common functions used in Bluesquare Data Science team",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BLSQ/data_pipelines",
    packages=["bluesquare_data_pipelines", "bluesquare_data_pipelines".access],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

#%%
