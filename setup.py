from setuptools import setup, find_packages


HYPEN_E_DOT = '-e .'
def get_requirements(file_path:str):
    requirements = []
    with open(file_path) as file_obj:
        requirements = file_obj.readlines()
        requirements = [req.replace("\n", "") for req in requirements]
        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)

setup(
    name= "StockDocApp",
    version= "0.0.1",
    author= "Spandan Maity",
    author_email= "spandanmaity58@gmail.com",
    packages=find_packages(),
    libraries=get_requirements('requirements.txt')
)