# Auto Join

echo "Dowloading AutoJoin"
rm -rf ./autojoin/original
rm -rf ./autojoin/datalake
git clone https://github.com/Yeye-He/Auto-Join.git ./autojoin/original
cd autojoin
mkdir -p datalake
sh ./create_datalake.sh
rm -rf ./autojoin/original
echo "AutoJoin Setup"
