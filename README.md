# Desafio 5
## Engenharia de dados e Airflow

Este é um repositório feito para o desafio de engenharia de dados do programa Lighthouse da Indicium. O desafio visa a criação de duas tasks em uma DAG no Airflow: a primeira deve ler a tabela "Order" no banco de dados Northwind (disponível na pasta data) e gerar um documento "output_orders.csv", já a segunda deveria ler esse arquivo gerado pela primeira task, fazer um join com a tabela "OrderDetail" do banco de dados Northwind e gerar um documento "count.txt" contendo a soma da quantidade vendida para o Rio de Janeiro. Após isso, bastava criar uma variável específica no airflow para que a terceira task fosse executada e gerasse um output final.

## Requisitos

Todos pacotes e biblitecas necessários para a instalação estão no arquivo requirements.txt. Para instalá-los, basta seguir o passo a passo:

1- Baixar o "virtualenv"
```sh
pip install virtualenv
```
2- Criar o ambiente virtual
```sh
virtualenv venv -p python3
```
3- Iniciar o ambiente
```sh
source venv/bin/activate
```
4- Instalar o conteúdo de requirements.txt
```sh
pip install -r /path/to/requirements.txt
```
sendo /path/to o caminho para o arquivo requirements.txt.

## Execução do Airflow

Para executar o airflow em seu ambiente, basta seguir o passo a passo:

1- Criar o arquivo .env no seu projeto
```sh
cat > .env 
```

Após isso arquivo abrirá no terminal como uma espécie de arquivo de texto.

2- Digitar o comando abaixo, pressionar Enter e pressionar Ctrl + z para sair do arquivo

```
export AIRFLOW_HOME=/home/seu_usuario/.../seu_projeto/
```

3- Executar o .env
```sh
source .env
```
4- Por fim, basta executar o Airflow
```sh
airflow standalone
```
