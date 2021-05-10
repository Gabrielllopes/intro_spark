# Fazendo o download dos dados
wget https://files.grouplens.org/datasets/movielens/ml-100k.zip --no-check-certificate
unzip ml-100k.zip

# convertendo para utf-8 e salvando em u_data.csv
iconv -f ASCII -t UTF-8 ml-100k/u.data > u_data.csv
iconv -f ISO-8859-1 -t UTF-8 ml-100k/u.item > u_item.csv
iconv -f ISO-8859-1 -t UTF-8 ml-100k/u.user > u_user.csv
