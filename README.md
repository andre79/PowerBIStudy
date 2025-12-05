# PowerBIStudy
This repository contains diferente datasource to connect in PowerBI for study.

## Docker Compose

### Sequência de Execução

Para iniciar o docker-compose pela primeira vez ou após resolver problemas de compatibilidade:

1. Remova containers e volumes antigos:

```bash
docker-compose down -v
```

2. Inicie o docker-compose:

```bash
docker-compose up
```

### Comandos Disponíveis

Para iniciar o docker-compose:

```bash
docker-compose up
```

Para executar em background:

```bash
docker-compose up -d
```

Para reconstruir as imagens antes de iniciar:

```bash
docker-compose up --build
```

## Importar Dados para PostgreSQL

### Visão Geral

O script `service/import_source.py` automatiza a importação de arquivos CSV e XLSX para o banco de dados PostgreSQL. Ele:

- Procura por arquivos `.csv` e `.xlsx` na pasta `src`
- Cria tabelas automaticamente com nomes baseados no arquivo
- Insere os dados nas tabelas
- Detecta tipos de dados e faz a conversão apropriada

### Função `main(drop_tables=False)`

Função principal que orquestra o processo de importação.

**Parâmetros:**
- `drop_tables` (bool): Controla se deseja apagar todas as tabelas antes de importar
  - `True`: Apaga todas as tabelas e reimporta
  - `False`: Mantém as tabelas existentes (padrão)

**Fluxo de execução:**
1. Conecta ao PostgreSQL (credenciais do docker-compose)
2. Se `drop_tables=True`, apaga todas as tabelas
3. Procura arquivos `.csv` e `.xlsx` na pasta `src`
4. Para cada arquivo:
   - Cria uma tabela: `TAB_{NOME_DO_ARQUIVO_MAIÚSCULA}`
   - Insere os dados na tabela
5. Desconecta do banco

**Exemplos de uso:**

```bash
# Importar sem apagar tabelas existentes
python service/import_source.py

# Importar apagando todas as tabelas primeiro
python -c "from service.import_source import main; main(drop_tables=True)"
```

### Apagar Todas as Tabelas

Para apagar todas as tabelas do banco de dados (sem reimportar):

```bash
python -c "from service.import_source import PostgreSQLImporter; i = PostgreSQLImporter(); i.connect(); i.drop_all_tables(); i.disconnect()"
```

**Aviso:** Este comando não pode ser desfeito! Todos os dados serão permanentemente perdidos.

### Mapeamento de Tipos de Dados

Os tipos pandas são convertidos para PostgreSQL:

| Pandas | PostgreSQL | Intervalo/Descrição |
|--------|-----------|---------------------|
| int64 | BIGINT | Inteiros: ±9.2 × 10^18 |
| int32 | INTEGER | Inteiros: ±2.1 × 10^9 |
| float64 | DECIMAL(15, 4) | Decimais com 15 dígitos, 4 casas |
| object | TEXT | Strings e objetos |
| bool | BOOLEAN | Valores lógicos |
| datetime64 | TIMESTAMP | Datas e horários |

### Convenção de Nomes de Tabelas

- **Formato:** `TAB_{NOME_DO_ARQUIVO_MAIÚSCULA}`
- **Exemplo:** `student_performance_analysis.csv` → `TAB_STUDENT_PERFORMANCE_ANALYSIS`

Nomes de colunas são automaticamente sanitizados:
- Convertidos para minúsculas
- Espaços substituídos por `_`
- Hífens substituídos por `_`
- Pontos substituídos por `_`

### Dependências

```bash
pip install psycopg2-binary pandas openpyxl
```

