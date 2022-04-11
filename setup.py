import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name = 'ftxhelperpy',
    version = '0.1.1',
    description = 'Python wrapper for interacting with the FTX api',
    url = 'https://github.com/BrianRyan94/ftxhelperpy',
    packages = setuptools.find_packages(),
    license = 'MIT',
    install_requires = [line.strip() for line in open("requirements.txt", "r").readlines()]
)
