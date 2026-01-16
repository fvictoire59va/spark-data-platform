#!/usr/bin/env python3
"""
Script pour générer des métriques en exerçant une charge système
"""

import hashlib
import threading
import time
from datetime import datetime


def cpu_intensive_work(duration_seconds=30):
    """Effectue du travail intensif CPU"""
    print(f"\n⚡ Démarrage charge CPU intensive ({duration_seconds}s)...")

    start = time.time()
    counter = 0

    while time.time() - start < duration_seconds:
        # Calculs intensifs
        data = str(counter).encode()
        for _ in range(1000):
            hashlib.sha256(data).hexdigest()
        counter += 1

    print(f"✅ Charge CPU terminée après {duration_seconds}s")


def memory_intensive_work(duration_seconds=30, size_mb=100):
    """Alloue et libère de la mémoire"""
    print(f"\n💾 Démarrage charge Mémoire ({size_mb}MB pendant {duration_seconds}s)...")

    start = time.time()
    chunk_size = 1024 * 1024  # 1MB

    while time.time() - start < duration_seconds:
        # Allouer de la mémoire
        data = [bytearray(chunk_size) for _ in range(size_mb)]
        time.sleep(0.5)

    print(f"✅ Charge Mémoire terminée après {duration_seconds}s")


def io_intensive_work(duration_seconds=30):
    """Effectue des opérations I/O"""
    print(f"\n📝 Démarrage charge I/O ({duration_seconds}s)...")

    start = time.time()
    counter = 0

    while time.time() - start < duration_seconds:
        filename = f"/tmp/test_{counter % 10}.txt"
        try:
            with open(filename, "w") as f:
                f.write("x" * 10000)
            counter += 1
        except:
            pass
        time.sleep(0.1)

    # Nettoyer
    for i in range(10):
        try:
            import os

            os.remove(f"/tmp/test_{i}.txt")
        except:
            pass

    print(f"✅ Charge I/O terminée après {duration_seconds}s")


def generate_metrics():
    """Génère plusieurs types de charge pour alimenter les métriques"""

    print("\n" + "=" * 70)
    print("⚡ GÉNÉRATEUR DE MÉTRIQUES SYSTÈME")
    print("=" * 70)
    print(f"⏰ Début : {datetime.now().strftime('%H:%M:%S')}")
    print("\n📊 Générant des métriques pour Prometheus...")

    # Lancer les charges en parallèle
    threads = [
        threading.Thread(target=cpu_intensive_work, args=(60,)),
        threading.Thread(target=memory_intensive_work, args=(60, 50)),
        threading.Thread(target=io_intensive_work, args=(60,)),
    ]

    for thread in threads:
        thread.start()

    # Attendre que tous les threads se terminent
    for thread in threads:
        thread.join()

    print("\n" + "=" * 70)
    print("✅ GÉNÉRATION DE MÉTRIQUES TERMINÉE")
    print("=" * 70)
    print(f"⏰ Fin : {datetime.now().strftime('%H:%M:%S')}\n")

    print("🌐 Vérifiez les dashboards Grafana:")
    print("   http://localhost:3000 (admin/spark123)")
    print("\n📈 Vous devriez voir:")
    print("   • CPU System Usage: ~100% pendant ~60 secondes")
    print("   • Memory Usage: Augmentation de ~50MB")
    print("   • Disk I/O: Activité d'écriture")
    print("\n💡 Si les graphiques restent vides:")
    print("   1. Attendez 30 secondes (délai de scrape Prometheus)")
    print("   2. Rafraîchissez la page (F5)")
    print("   3. Vérifiez : http://localhost:9090 (Prometheus)")


if __name__ == "__main__":
    generate_metrics()
