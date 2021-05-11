## Tutorial de Spark

Este repositório é um tutorial de introdução a spark escrito em python(pyspark).
Para fins de demonstração será utilizado o dataset aberto [MovieLens 100k](https://grouplens.org/datasets/movielens/100k/).

Este dataset possui 100k avaliações de 1000 usuários em 1700 filmes e foi lançado em 1998. Seus dados estão distribuidos em 20 arquivos diferentes, sendo que para o propósito deste tutorial serão utilizados apenas 3:  
* u.data (id do usuário, id do filme, nota, data)
* u.item (id do filme, título, gêneros) (uma coluna para cada gênero, com 1 se o filme pertence ao gênero)
* u.user (id do usuário e informações demográficas)

### Passo 1 - Adiquirindo e tratando os dados

Os dados podem ser obtidos [aqui](https://files.grouplens.org/datasets/movielens/ml-100k.zip).

Pyspark trabalha com dados no formato UTF-8, e estes dados estão em: ASCII (u.data) e ISO-8859 (u.item, u.user).Logo o primeiro passo é converter eles para UTF-8.

Caso esteja utilizando linux ou mac isso pode ser feito com o comando iconv, que é demontrado abaixo.

```bash
# convertendo para utf-8 e salvando em u_data.csv
iconv -f ASCII -t UTF-8 u.data > u_data.csv
iconv -f ISO-8859-1 -t UTF-8 u.item > u_item.csv
iconv -f ISO-8859-1 -t UTF-8 u.user > u_user.csv
```

O script `get_data.sh` realiza o download dos dados, estração e conversão deles para utf-8, para executalo basta executar o segunte comando:  
```bash
$ sh get_data.sh
```

### Passo 2 - Criar data lake
Aqui sera criado um mini datalake, que será um arquivo parquet que contem todas as informações de u.data, u.item e u.user agregadas. Este arquivo sera formatado para que cada campo possua um tipo.

É preciso atenção pois os separadores estão em formatos diferêntes, sendo que u_item e u_usar usam "|" como separador e u_data usa "\t".

A criação deste arquivo parquet pode ser observada no nootebook `create_parquet.ipynb` assim como no script `create_parquet.py`.

