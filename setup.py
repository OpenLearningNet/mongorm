from setuptools import setup, find_packages

setup(name='mongorm',
      version='0.5.4',
      packages=find_packages(),
      author='Theo Julienne, John Garland',
      author_email='theo@icy.com.au, john@openlearning.com',
      url='https://github.com/OpenLearningNet/mongorm',
      license='MIT',
      include_package_data=True,
      description='Mongorm',
      long_description='Mongorm',
      platforms=['any'],
      install_requires=['pymongo >= 2.4.2', 'pysignals'],
)
