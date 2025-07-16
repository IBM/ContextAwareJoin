from setuptools import setup

setup(
   name='ContextAwareJoin',
   packages=['exact_match','topjoin','warpgate','LSH_ensemble','deepjoin','myutils'],
   package_dir={'':'src'},
   install_requires=[
      "gitpython==3.1.41",
      "pandas",
      "tqdm",
      "datasketch==1.6.5",
      "fasttext",
      "scikit-learn==1.4.0",
      "scipy==1.12.0",
      "xxhash==3.4.1",
      "huggingface-hub==0.20.3",
      "sentence-transformers==2.2.2",
      "hnswlib==0.8.0",
      "sqlalchemy==2.0.25"
   ],
)