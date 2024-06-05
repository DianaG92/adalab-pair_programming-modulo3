# Pair ETL I
# Ejercicios ETL Parte I
# NOTA Este ejercicio debe realizarse en un archivo .py
# En este caso trabajas en una empresa de venta al por menor de productos italianos y debes realizar la limpieza, transformación e integración de datos de ventas, productos y clientes para su análisis.
# Los pasos que deberás seguir en este ejercicio son:
# 1. Lectura de la Información:
    # Leer los archivos CSV (ventas.csv, productos.csv, clientes.csv).
#%%
import pandas as pd
import numpy as np

#%%
from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import KNNImputer

#%%
pd.set_option("display.max_columns", None)

#%%
import warnings
warnings.filterwarnings("ignore")

#%%
df_ventas = pd.read_csv("ventas.csv", index_col=0)
df_ventas.head()

#%%
df_productos = pd.read_csv("productos.csv", on_bad_lines="skip")
df_productos.head()

#%%
df_clientes = pd.read_csv("clientes.csv", index_col=0)
df_clientes.head()

    # Explorar los conjuntos de datos para comprender su estructura, columnas, tipos de datos, etc.
#%%
def exploracion_dataframe(dataframe):
    
    print(f"Los duplicados que tenemos en el conjunto de datos son: {dataframe.duplicated().sum()}")
    print("\n ..................... \n")
    
    # Obtenemos la inforamción general del DataFrame
    print("La información que tenemos de nuestro DataFrame es:")
    display(dataframe.info())
    
    # Generamos un DataFrame para los valores nulos
    print("Los nulos que tenemos en el conjunto de datos son:")
    df_nulos = pd.DataFrame(dataframe.isnull().sum() / dataframe.shape[0] * 100, columns = ["%_nulos"])
    display(df_nulos[df_nulos["%_nulos"] > 0])
    
    print("\n ..................... \n")
    print(f"Los tipos de las columnas son:")
    display(pd.DataFrame(dataframe.dtypes, columns = ["tipo_dato"]))
    
    # Sacamos valores únicos de columnas categóricas
    print("\n ..................... \n")
    print("La cantidad de valores únicos que tenemos para las columnas categóricas son: ")
    dataframe_categoricas = dataframe.select_dtypes(include = "O")
    for col in dataframe_categoricas.columns:
        print(f"La columna {col.upper()} tiene los siguientes valores únicos:")
        display(pd.DataFrame(dataframe[col].value_counts()).head())    

    # Sacamos valores únicos de columnas numéricas
    print("\n ..................... \n")
    print("La cantidad de valores únicos que tenemos para las columnas numéricas son: ")
    dataframe_numericas = dataframe.select_dtypes(include = np.number)
    for col in dataframe_numericas.columns:
        print(f"La columna {col.upper()} tiene los siguientes valores únicos:")
        display(pd.DataFrame(dataframe[col].value_counts()).head()) 

#%%        
exploracion_dataframe(df_ventas)        

#%%
exploracion_dataframe(df_productos) 

#%%
exploracion_dataframe(df_clientes) 

# 2. Transformación de Datos:
    # Limpiar los datos: manejar valores nulos, eliminar duplicados si los hay, corregir errores tipográficos, etc.

# df_ventas:
# fecha (pasar de object a date)
# df_productos:
# categoria es float
# precio (pasar de object a float)
# origen es object


    # Realizar la integración de datos: unir los conjuntos de datos apropiados para obtener una tabla única que contenga información de ventas junto con detalles de productos y clientes.




    # Aplicar transformaciones relevantes según sea necesario: por ejemplo, convertir tipos de datos, renombrar columnas, crear nuevas características derivadas, etc.


# %%
