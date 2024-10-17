# UCU_6K1S_MiningMassiveDatasets

# Setup 
- Install Java11:
    - Step 1: Install openjdk by running `brew install openjdk@11`
    - Step 2: Create symlink by running `sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk`
    - Step 3: Add `export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"` to the end of `~/.zshrc`
- Create venv: `python11 -m venv venv`
- Install requirements: `pip install --upgrade -r requirements.txt`


# Class Notebooks:
- https://colab.research.google.com/drive/1cgBHmUrz5bT7OiDZdB-yuYOo00oLwMxT?usp=sharing
- Bloom Filter: https://colab.research.google.com/drive/1P1zpA_c80bWthrP2xuqFQzi-EzJQzZ9H?usp=sharing#scrollTo=q2b_ZDMiIE_c
- Fajolet - Martin Algorithm: https://colab.research.google.com/drive/1bosF030yo-HvgwWpcuCaEKORtWYuVWx_?usp=sharing#scrollTo=3JpZMysG5Hjl
- Wikipedia recent Changes feed: https://colab.research.google.com/drive/1mUBo48oaUIqqZFUZouAtJ8EVKfQBm23R?usp=sharing

# Class Additional Resources:
- https://huggingface.co/sentence-transformers/LaBSE
- http://insideairbnb.com/get-the-data.html
- https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StopWordsRemover.html
- https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.HashingTF.html
- https://spark.apache.org/docs/latest/mllib-feature-extraction.html
- https://spark.apache.org/docs/latest/ml-tuning.html
- http://listen.hatnote.com/

Lecture Slides are located in `class_materials` directory# UCU_6K1S_MiningMassiveDatasets
