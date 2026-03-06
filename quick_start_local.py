"""
Quick Start Pipeline - Versión Local
Procesa los datos sin necesidad de HDFS, directamente desde archivos locales.
Útil para desarrollo y pruebas rápidas.
"""

import subprocess
import sys
import time
import os

def ejecutar_script(script_name):
    print(f"--- Iniciando: {script_name} ---")
    try:
        resultado = subprocess.run(['/opt/spark/bin/spark-submit', script_name], check=True)
        if resultado.returncode == 0:
            print(f"--- Finalizado con éxito: {script_name} ---\n")
    except subprocess.CalledProcessError as e:
        print(f"!!! ERROR en {script_name}: El pipeline se ha detenido !!!")
        sys.exit(1)

def verificar_archivo_datos():
    """Verifica que el archivo de datos existe"""
    if not os.path.exists('data/raw/FoodImports.csv'):
        print("❌ ERROR: Archivo de datos no encontrado: data/raw/FoodImports.csv")
        sys.exit(1)
    print("✅ Archivo de datos encontrado: data/raw/FoodImports.csv\n")

if __name__ == "__main__":
    start_time = time.time()
    
    print("="*60)
    print("🚀 QUICK START - PIPELINE LOCAL (SIN HDFS)")
    print("="*60 + "\n")
    
    print("⚠️  Modo LOCAL - Los datos se procesarán sin HDFS")
    print("📝 Los datos se guardarán en: data/processed/local/\n")
    
    # Verificar que tenemos los datos
    verificar_archivo_datos()
    
    # Crear directorio de salida si no existe
    os.makedirs('data/processed/local', exist_ok=True)
    
    # 1. Transformación de Bronze a Silver (Limpieza y Casteo)
    print("1️⃣ PROCESANDO: Silver Transformation (Limpieza de datos)")
    ejecutar_script('src/processing/silver_transformation_local.py')
    
    # 2. Transformación de Silver a Gold (KPIs y Agregaciones)
    print("2️⃣ PROCESANDO: Gold Aggregation (Análisis y KPIs)")
    ejecutar_script('src/processing/gold_aggregation_local.py')
    
    end_time = time.time()
    duracion = (end_time - start_time) / 60
    
    print("="*60)
    print(f"✅ PIPELINE COMPLETADO EXITOSAMENTE en {duracion:.2f} minutos")
    print("="*60)
    print("\n📊 Resultados disponibles en: data/processed/local/")
    print("\nPróximos pasos:")
    print("  1. Revisar los datos procesados en data/processed/local/")
    print("  2. Para usar HDFS, configura SSH sin contraseña:")
    print("     ssh-keygen -t rsa && ssh-copy-id -i ~/.ssh/id_rsa.pub localhost")
    print("  3. Luego ejecuta: python3 main_pipeline.py")
