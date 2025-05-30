# COMPREHENSIVE SPARK ENVIRONMENT DIAGNOSTICS
# Run this to identify the root cause

import os
import sys
import subprocess
import platform


def check_java_environment():
    """Check Java version and environment."""
    print("=== JAVA ENVIRONMENT CHECK ===")

    try:
        # Check Java version
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        java_version = result.stderr.split('\n')[0] if result.stderr else "Unknown"
        print(f"Java version: {java_version}")

        # Check JAVA_HOME
        java_home = os.environ.get('JAVA_HOME', 'Not set')
        print(f"JAVA_HOME: {java_home}")

        # Check if Java is 64-bit
        result_arch = subprocess.run(['java', '-d64', '-version'], capture_output=True, text=True)
        if result_arch.returncode == 0:
            print("✅ Java is 64-bit")
        else:
            print("❌ Java might not be 64-bit")

        return True

    except FileNotFoundError:
        print("❌ Java not found in PATH")
        return False
    except Exception as e:
        print(f"❌ Java check failed: {e}")
        return False


def check_python_environment():
    """Check Python and system environment."""
    print("\n=== PYTHON ENVIRONMENT CHECK ===")

    print(f"Python version: {sys.version}")
    print(f"Platform: {platform.platform()}")
    print(f"Architecture: {platform.architecture()}")
    print(f"Machine: {platform.machine()}")

    # Check if we're in a virtual environment
    venv = os.environ.get('VIRTUAL_ENV', 'Not in virtual environment')
    print(f"Virtual environment: {venv}")


def check_pyspark_installation():
    """Check PySpark installation details."""
    print("\n=== PYSPARK INSTALLATION CHECK ===")

    try:
        import pyspark
        print(f"PySpark version: {pyspark.__version__}")
        print(f"PySpark location: {pyspark.__file__}")

        # Check if Spark JARs exist
        spark_home = pyspark.__file__.replace('__init__.py', '')
        jars_dir = os.path.join(spark_home, 'jars')

        if os.path.exists(jars_dir):
            jar_count = len([f for f in os.listdir(jars_dir) if f.endswith('.jar')])
            print(f"✅ Spark JARs directory exists with {jar_count} JAR files")
        else:
            print("❌ Spark JARs directory not found")

        return True

    except ImportError as e:
        print(f"❌ PySpark import failed: {e}")
        return False


def check_memory_settings():
    """Check system memory and Java memory settings."""
    print("\n=== MEMORY SETTINGS CHECK ===")

    try:
        # Check system memory
        if platform.system() == "Linux":
            with open('/proc/meminfo', 'r') as f:
                meminfo = f.read()
                for line in meminfo.split('\n'):
                    if 'MemTotal' in line:
                        total_mem = line.split()[1]
                        total_gb = int(total_mem) / 1024 / 1024
                        print(f"Total system memory: {total_gb:.1f} GB")
                        break

        # Check Java memory settings
        java_opts = os.environ.get('JAVA_OPTS', 'Not set')
        spark_driver_memory = os.environ.get('SPARK_DRIVER_MEMORY', 'Not set')

        print(f"JAVA_OPTS: {java_opts}")
        print(f"SPARK_DRIVER_MEMORY: {spark_driver_memory}")

    except Exception as e:
        print(f"Memory check failed: {e}")


def test_basic_java_spark():
    """Test if we can run basic Java/Spark operations."""
    print("\n=== BASIC SPARK TEST ===")

    try:
        # Test minimal Spark creation
        from pyspark import SparkConf, SparkContext

        print("Attempting to create minimal SparkContext...")

        conf = SparkConf()
        conf.setAppName("DiagnosticTest")
        conf.setMaster("local[1]")  # Single thread to minimize issues
        conf.set("spark.sql.adaptive.enabled", "false")
        conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")  # Avoid Kryo issues

        sc = SparkContext(conf=conf)
        print("✅ SparkContext created successfully")

        # Test basic operation
        rdd = sc.parallelize([1, 2, 3, 4])
        result = rdd.collect()
        print(f"✅ Basic RDD operation successful: {result}")

        sc.stop()
        print("✅ SparkContext stopped successfully")
        return True

    except Exception as e:
        print(f"❌ Basic Spark test failed: {e}")
        return False


def suggest_fixes():
    """Suggest potential fixes based on diagnostics."""
    print("\n=== SUGGESTED FIXES ===")

    print("1. **Java Version Issue**:")
    print("   - PySpark 3.4 requires Java 8, 11, or 17")
    print("   - If using Java 21+, downgrade to Java 17")
    print("   - Command: sudo apt install openjdk-17-jdk (Ubuntu)")

    print("\n2. **Memory Issues**:")
    print("   - Set Java memory limits:")
    print("   - export JAVA_OPTS='-Xmx2g -Xms1g'")
    print("   - export SPARK_DRIVER_MEMORY='2g'")

    print("\n3. **PySpark Reinstall**:")
    print("   - pip uninstall pyspark")
    print("   - pip install pyspark==3.4.3 --no-cache-dir")

    print("\n4. **Alternative: Use PySpark 3.3**:")
    print("   - pip uninstall pyspark")
    print("   - pip install pyspark==3.3.4")
    print("   - Use: iceberg-spark-runtime-3.3_2.12:1.4.3")

    print("\n5. **Use Conda Instead of Pip**:")
    print("   - conda install pyspark=3.4.3")
    print("   - Conda often has better Java integration")


def run_full_diagnostics():
    """Run all diagnostic checks."""
    print("🔍 RUNNING FULL SPARK DIAGNOSTICS...")
    print("=" * 50)

    java_ok = check_java_environment()
    check_python_environment()
    pyspark_ok = check_pyspark_installation()
    check_memory_settings()

    if java_ok and pyspark_ok:
        spark_ok = test_basic_java_spark()
        if not spark_ok:
            print("\n❌ SPARK INITIALIZATION FAILED")
            suggest_fixes()
    else:
        print("\n❌ PREREQUISITE CHECKS FAILED")
        suggest_fixes()


# Run the diagnostics
if __name__ == "__main__":
    run_full_diagnostics()