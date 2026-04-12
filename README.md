# Radar Combustível — MongoDB (seed)

Material de apoio ao trabalho final: base documental no **MongoDB** para o caso **Radar Combustível**, com script de carga usando **Faker** e **Docker Compose** para subir o servidor localmente.

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose
- Python 3.10 ou superior
- `pip` para instalar dependências

## Início rápido

Na pasta deste repositório (`Trabalho_final`):

1. **Subir o MongoDB**

   ```bash
   docker compose up -d
   ```

2. **Instalar dependências Python**

   ```bash
   pip install -r requirements.txt
   ```

3. **Popular o banco** (100 mil documentos por coleção; ajustável por variável de ambiente)

   ```bash
   python seed_radar_combustivel.py
   ```

O script remove e recria as coleções a cada execução, para manter o seed reproduzível.

## Banco e coleções

| Banco (padrão)     | Descrição                          |
|--------------------|------------------------------------|
| `radar_combustivel` | Base usada pelo seed               |

| Coleção                 | Conteúdo principal                                      |
|-------------------------|---------------------------------------------------------|
| `postos`                | Cadastro de postos, endereço e `location` (GeoJSON)     |
| `eventos_preco`         | Atualizações de preço por posto e tipo de combustível   |
| `buscas_usuarios`       | Buscas, filtros e métricas de consulta                  |
| `avaliacoes_interacoes` | Avaliações, favoritos e outras interações                 |
| `localizacoes_postos`   | Localização indexável (geo + município/IBGE), 1:1 com posto |

Após a carga, o script cria **índices** (incluindo geoespacial em `postos.location` e `localizacoes_postos.geo`).

## Variáveis de ambiente (opcional)

| Variável    | Padrão                         | Significado                          |
|-------------|--------------------------------|--------------------------------------|
| `MONGO_URI` | `mongodb://localhost:27017`    | URI de conexão do MongoDB            |
| `DB_NAME`   | `radar_combustivel`            | Nome do database                     |
| `SEED`      | `42`                           | Semente para dados reproduzíveis     |
| `BATCH_SIZE`| `5000`                         | Tamanho do lote em `insert_many`     |
| `N`         | `100000`                       | Quantidade de documentos **por coleção** |

Exemplo com volume menor para testes:

```bash
set N=1000
python seed_radar_combustivel.py
```

No Linux ou macOS, use `export N=1000`.

## Docker — comandos úteis

- Parar os containers: `docker compose down`
- Parar e remover o volume persistente dos dados: `docker compose down -v`

## Observação

A carga com `N=100000` em **cinco** coleções gera centenas de milhares de documentos e pode levar vários minutos e espaço em disco relevante, conforme a máquina. Reduza `N` durante o desenvolvimento.

## Referência do trabalho

O enunciado completo está em `../trabalho_final/Trabalho-final-IMDB.md` (raiz do repositório de apostilas).
