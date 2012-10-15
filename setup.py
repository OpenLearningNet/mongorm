from setuptools import setup, find_packages

setup(name='mongorm',
      version='0.3.3',
      packages=find_packages(),
      author='Theo Julienne',
      author_email='theo@icy.com.au',
      url='http://www.icy.com.au/???',
      license='MIT',
      include_package_data=True,
      description='Mongorm',
      long_description='Mongorm',
      platforms=['any'],
      install_requires=['pymongo', 'pysignals'],
)
