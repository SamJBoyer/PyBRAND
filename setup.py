from setuptools import setup, find_packages

setup(
    name='PyBRAND',
    version='0.1.0',
    description='Python tools for building BRAND nodes in Python',
    author='Samuel James Boyer',
    author_email='sam.james.boyer@gmail.com',
    url='https://github.com/yourusername/pybrand',
    packages=find_packages(),
    zip_safe=False,  # Set to False if you want to install via an egg link
)