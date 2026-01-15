"""Test rapide de la configuration Airflow."""
import sys
from pathlib import Path

# Ajouter le répertoire des DAGs au chemin
dag_dir = Path(__file__).parent / "orchestration" / "airflow" / "dags"
sys.path.insert(0, str(dag_dir))

def test_dag_syntax():
    """Vérifier la syntaxe du DAG."""
    try:
        # Importer le DAG
        from sales_pipeline_dag import dag

        print("✅ DAG importé avec succès")
        print(f"   DAG ID: {dag.dag_id}")
        print(f"   Description: {dag.description}")
        print(f"   Schedule: {dag.schedule_interval}")
        print(f"   Nombre de tasks: {len(dag.tasks)}")

        # Afficher les tasks
        print("\n📋 Tasks du DAG:")
        for task in dag.tasks:
            print(f"   - {task.task_id}")

        print("\n✅ Configuration Airflow valide!")
        return 0

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(test_dag_syntax())
