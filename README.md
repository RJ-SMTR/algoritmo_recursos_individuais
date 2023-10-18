# Algoritmo de Classificação de Recursos (ACRe)

---

Desenvolvido por: Igor Laltuf


> Status: Em andamento


## Descrição

O algoritmo presente neste repositório tem como objetivo classificar as viagens individuais presentes nos recursos de pagamento do subsídio. 

## Estrutura do repositório

```
├── README.md                  <- Descrição do resumo da análise
├── data
│   ├── output                 <- Dados finais (tabelas de resumo e afins)
│   ├── treated                <- Dados tratados
│   ├── figures                <- Imagens geradas da análise
│   └── raw                    <- Arquivo com os dados da amostra no formato .xlsx
├── scripts                    <- Scripts Python
│   ├── run.py                 <- Script que executa o algoritmo
│   ├── set_credentials.py     <- Configurações das credenciais do Big Query
│   ├── log                    <- Log files
|   ├── queries                <- Scripts de queries
│   └── data_processing        <- Scripts específicos para pré-processamento de dados
└── requirements.txt           <- Pacotes específicos da análise

```


O funcionamento do algoritmo ocorre conforme mostram as figuras abaixo:

<img src="./data/figures/algoritmo parte 1.jpg" alt="Descrição da imagem" width="800"/>

<img src="./data/figures/algoritmo parte 2.jpg" alt="Descrição da imagem" width="800"/>


O algoritmo recebe um arquivo contendo informações sobre as viagens e retorna um status, que pode ser:




- Viagem duplicada na amostra
- Viagem identificada e já paga
- Viagem identificada e já paga para serviço diferente da amostra
- Viagem inválida - Não atingiu % de GPS ou trajeto correto
- Viagem inválida - Não atingiu % de GPS ou trajeto correto para serviço diferente da amostra
- Sinal de GPS encontrado para o veículo operando no mesmo serviço da amostra
- Sinal de GPS encontrado para o veículo operando em serviço diferente da amostra
- Sinal de GPS encontrado, mas veículo não passou no raio de 500m do ponto de partida/final do trajeto
- Sinal de GPS não encontrado para o veículo no horário da viagem


Por fim, para os casos em que os dados do sinal de GPS são encontrados para o serviço da amostra no momento da viagem, são gerados mapas que comparam os sinais de GPS com o trajeto. Estes mapas ficam disponibilizados no diretório `data/output/maps`:

<img src="./data/figures/mapa_exemplo.png" alt="Descrição da imagem" width="800"/>



## Modo de Usar

### 1. Preparar o ambiente

```bash
python -m venv env
. env/bin/activate # no Windows, usar . env/Scripts/activate
python -m pip install --upgrade pip 
pip install -r requirements.txt
```


* Configure suas credenciais para leitura/escrita no datalake:
Preencha suas credencias no arquivo `scripts/set_credentials.py`



### 2. Arquivo de input

A pasta `data/raw` deve conter um arquivo no formato xlsx contendo os dados das viagens individuais que serão avaliadas pelo algoritmo. O arquivo deve conter apenas uma aba e as seguintes colunas:

<img src="./data/figures/tabela_input.png" alt="Descrição da imagem" width="800"/>


Sobre os dados do arquivo:
- a coluna id_veiculo não deve conter o dígito antes do número do veículo;
- a coluna sentido deve existir, mesmo que esteja vazia; e
- os dados do arquivo `arquivo_de_exemplo.xlsx` no diretório `data/raw` devem ser usados apenas para fins de testes do algoritmo. Os dados das viagens que constam no arquivo foram alterados manualmente e não devem ser considerados para análises sobre as respectivas viagens.
- A coluna flag_reprocessamento indica se o recurso deve ser reprocessado utilizando o código do serviço que consta no recurso nos sinais de GPS identificados no momento da viagem. Isto é válido apenas para as viagens até 16/11/2022.


### 3. Como executar o algoritmo

* Execute o arquivo `run.py` para iniciar o algoritmo:

```bash
python scripts/run.py
```

Após a primeira execução do algoritmo, é possível executá-lo novamente com a flag `--cache` para reutilizar os dados que foram baixados na execução anterior.
```bash
python scripts/run.py --cache
```

