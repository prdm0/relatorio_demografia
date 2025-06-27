import polars as pl
import numpy as np
from datetime import date, datetime, timedelta
from polars import selectors as sel

# Para garantir que os dados "aleatórios" sejam sempre os mesmos
np.random.seed(42)

# Definindo o número de linhas
n_rows = 150

# --- Gerando os Dados ---
# Lista de produtos com alguns nomes "sujos"
produtos_lista = [
    "Laptop Gamer XZ-1000", "  Mouse Óptico R-5  ", "Teclado Mecânico K-90",
    "Monitor UltraWide 34'", "Cadeira Gamer Confort", "Webcam 4K Pro",
    "SSD 1TB Rápido", "Headset 7.1 Surround"
]

# Lista de status com capitalização inconsistente
status_lista = ["Entregue", "Enviado", "Cancelado", "enviado", "Pendente"]

# --- Criando o DataFrame Polars ---
df = pl.DataFrame({
    "id_pedido": range(1, n_rows + 1),
    "id_cliente": np.random.randint(101, 151, size=n_rows),
    "data_pedido": pl.date_range(
        date(2025, 1, 15), date(2025, 6, 15),
        interval="1d", eager=True
    ).sample(n=n_rows, with_replacement=True).sort(),
    "produto_info": [
        {
            "nome": np.random.choice(produtos_lista),
            "categoria": np.random.choice(["Eletrônicos", "Acessórios", "Periféricos"]),
            "sku": f"SKU-{np.random.randint(1000, 9999)}"
        } for _ in range(n_rows)
    ],
    "quantidade": np.random.randint(1, 5, size=n_rows),
    "preco_unitario": (np.random.lognormal(4, 0.8, size=n_rows) * 50).round(2),
    "status_pedido": np.random.choice(status_lista, size=n_rows, p=[0.6, 0.15, 0.1, 0.1, 0.05]),
    "custo_frete": np.where(
        np.random.rand(n_rows) > 0.85,
        None,
        (np.random.rand(n_rows) * 50).round(2)
    ),
    "pagamento_confirmado": np.random.choice([True, False, None], size=n_rows, p=[0.8, 0.15, 0.05]),
    "avaliacao_cliente": np.random.choice([1, 2, 3, 4, 5, None], size=n_rows, p=[0.05, 0.05, 0.1, 0.3, 0.4, 0.1]),
    "regiao_entrega": np.random.choice(["Sudeste", "Nordeste", "Sul", "Centro-Oeste", "Norte"], size=n_rows)
}).with_columns(
    # Convertendo tipos para os mais adequados (prática recomendada)
    pl.col("regiao_entrega").cast(pl.Categorical),
    pl.col("status_pedido").cast(pl.Categorical)
)


a = (
    df
    .select(~sel.starts_with("id_"))
    .group_by(["status_pedido"])
    .agg(pl.count().alias("Total de Pedidos"))
)
