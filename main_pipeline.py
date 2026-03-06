import subprocess
import sys
import time

def ejecutar_script(script_name):
    print(f"--- Iniciando: {script_name} ---")
    try:
        # Usamos spark-submit para ejecutar los archivos de PySpark con HDFS
        resultado = subprocess.run(['/opt/spark/bin/spark-submit', script_name], check=True)
        if resultado.returncode == 0:
            print(f"--- Finalizado con éxito: {script_name} ---\n")
    except subprocess.CalledProcessError as e:
        print(f"!!! ERROR en {script_name}: El pipeline se ha detenido !!!")
        sys.exit(1)

if __name__ == "__main__":
    start_time = time.time()
    
    print("="*70)
    print("🚀 INICIANDO PIPELINE END-TO-END: FOOD IMPORTS (CON HDFS)")
    print("="*70 + "\n")
    
    print("⚠️  Este pipeline requiere HDFS activo")
    print("📝 Para iniciar HDFS, ejecuta: /opt/hadoop/sbin/start-dfs.sh\n")

    # 1. Ingesta Bronze - Cargar CSV a HDFS
    print("1️⃣ PROCESANDO: Bronze Ingestion (Carga a HDFS)")
    ejecutar_script('src/ingestion/bronze_ingestion.py')

    # 2. Transformación de Bronze a Silver (Limpieza y Casteo)
    print("2️⃣ PROCESANDO: Silver Transformation")
    ejecutar_script('src/processing/silver_transformation.py')

    # 3. Transformación de Silver a Gold (KPIs y Agregaciones)
    print("3️⃣ PROCESANDO: Gold Aggregation (con MongoDB y Hive)")
    ejecutar_script('src/processing/poblar_capa_functional.py')

    end_time = time.time()
    duracion = (end_time - start_time) / 60
    
    print("="*70)
    print(f"✅ PIPELINE COMPLETADO EXITOSAMENTE en {duracion:.2f} minutos")
    print("="*70)