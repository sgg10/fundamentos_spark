import os
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    Row
)

base = os.path.abspath(os.pardir)
data_dir = os.path.join(base, "work", "files")


def get_schemas():
    return {
        "deporte": StructType([
            StructField("deporte_id", IntegerType(), False),
            StructField("deporte", StringType(), False)
        ]),
        "deportista": StructType([
            StructField("deportista_id", IntegerType(), False),
            StructField("nombre", StringType(), False),
            StructField("genero", StringType(), False),
            StructField("edad", IntegerType(), False),
            StructField("altura", IntegerType(), False),
            StructField("peso", FloatType(), False),
            StructField("equipo_id", IntegerType(), False)
        ]),
        "evento": StructType([
            StructField("evento_id", IntegerType(), False),
            StructField("evento", StringType(), False),
            StructField("deporte_id", IntegerType(), False)
        ]),
        "juegos": StructType([
            StructField("juego_id", IntegerType(), False),
            StructField("nombre_juego", StringType(), False),
            StructField("anio", IntegerType(), False),
            StructField("temporada", StringType(), False),
            StructField("ciudad", StringType(), False)
        ]),
        "paises": StructType([
            StructField("pais_id", IntegerType(), False),
            StructField("equipo", StringType(), False),
            StructField("sigla", StringType(), False)
        ]),
        "resultados": StructType([
            StructField("resultado_id", IntegerType(), False),
            StructField("medalla", StringType(), False),
            StructField("deportista_id", IntegerType(), False),
            StructField("juego_id", IntegerType(), False),
            StructField("evento_id", IntegerType(), False)
        ]),
        "deportista_error": StructType([
            StructField("deportista_id", StringType(), False),
            StructField("nombre", StringType(), False),
            StructField("genero", StringType(), False),
            StructField("edad", StringType(), False),
            StructField("altura", StringType(), False),
            StructField("peso", StringType(), False),
            StructField("equipo_id", StringType(), False)
        ])
    }

def load_dataframes_from_schemas(sql_context, schemas=get_schemas()):
    dp1 = sql_context.read.schema(schemas['deportista']).option("header", "true").csv(f"{data_dir}/deportista.csv")
    dp2 = sql_context.read.schema(schemas['deportista']).option("header", "false").csv(f"{data_dir}/deportista2.csv")

    dataframes = {
        "deporte": sql_context.read.schema(schemas['deporte']).option("header", "true").csv(f"{data_dir}/deporte.csv"),
        "deportista": dp1.union(dp2),
        "evento": sql_context.read.schema(schemas['evento']).option("header", "true").csv(f"{data_dir}/evento.csv"),
        "juegos": sql_context.read.schema(schemas['juegos']).option("header", "true").csv(f"{data_dir}/juegos.csv"),
        "paises": sql_context.read.schema(schemas['paises']).option("header", "true").csv(f"{data_dir}/paises.csv"),
        "resultados": sql_context.read.schema(schemas['resultados']).option("header", "true").csv(f"{data_dir}/resultados.csv"),
        "deportista_error": sql_context.read.option("header", "true").csv(f"{data_dir}/deportistaError.csv", schema=schemas['deportista_error'])
    }
    del(dp1, dp2)

    return dataframes
