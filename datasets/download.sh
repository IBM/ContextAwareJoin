# Auto Join

echo "Dowloading AutoJoin"
rm -rf ./autojoin/original
rm -rf ./autojoin/datalake
git clone https://github.com/Yeye-He/Auto-Join.git ./autojoin/original
cd autojoin
mkdir -p datalake
sh ./create_datalake.sh
rm -rf ./autojoin/original
echo "AutoJoin Setup Done"

cd ..

# Wiki Join

echo "Downloading WikiJoin"
rm -rf ./wikijoin/wiki-join-search
rm -rf ./wikijoin/datalake
rm ./wikijoin/gt.jsonl
wget -O ./wikijoin/original.tar.bz2 https://zenodo.org/records/10042019/files/wiki-join-search.tar.bz2
echo "Setting up Wiki Join"
cd wikijoin
tar -xvzf gt.jsonl.tar.gz
mkdir ./datalake
tar -xjf ./original.tar.bz2
mv wiki-join-search/tables-with-headers/ datalake
rm -r wiki-join-search
rm -r ./original.tar.bz2
echo "WikiJoin Setup Done"

cd ..
# Go Sales

echo "Downloading GO Sales"
wget -O ./gosales/Extended_Samples_11_1_1.zip https://public.dhe.ibm.com/software/data/sw-library/cognos/mobile/deployments/Extended_Samples_11_1_1.zip
wget -O ./gosales/Extended_Samples_11_1_2.zip https://public.dhe.ibm.com/software/data/sw-library/cognos/mobile/deployments/Extended_Samples_11_1_2.zip

echo "=====Manual intervention needed====="
echo "Use Either MS-SQL , IBM-DB2 or Oracle to load the dataset and dump CSVs to the datalake folder"

echo "GOSales Setup Done"

# Open Data

echo "Downloading OpenData"
wget https://zenodo.org/records/15881731/files/opendata-contextawarejoins.zip
unzip opendata-contextawarejoins.zip 
mv opendata-contextawarejoins opendata
rm opendata-contextawarejoins.zip 
echo "OpenData Setup Done"

# NextIA
wget -O testbedS.zip https://mydisk.cs.upc.edu/s/dX3FajwWZn7rrrd/download
unzip testbedS.zip
mv testbedS/datasets ./nextia/testbedS/datalake
rm -rf testbedS
rm testbedS.zip

wget -O testbedM.zip https://mydisk.cs.upc.edu/s/niPyR4WTtxydprj/download
unzip testbedM.zip
mv testbedM/datasets ./nextia/testbedM/datalake
rm -rf testbedM
rm testbedM.zip

# Valentine
wget -O Valentine-datasets.zip "https://zenodo.org/records/5084605/files/Valentine-datasets.zip?download=1"
unzip Valentine-datasets.zip
mv Valentine-datasets ./valentine
for f in ./valentine/Valentine-datasets/*/Semantically-Joinable/*/*mapping*.json;do `python convert_valentine_gt.py $f`;done
rm Valentine-datasets.zip


