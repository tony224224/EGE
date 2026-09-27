import os
import time
import sqlite3
import subprocess
import json

def repo_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

DB_PATH = os.path.join(os.path.dirname(__file__), 'rename_queue.db')
LOCK_FILE = os.path.join(os.path.dirname(__file__), 'worker.lock')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY, 
            task_dir TEXT, 
            files TEXT, 
            commit_msg TEXT,
            unique_key TEXT UNIQUE ON CONFLICT REPLACE
        )
    ''')
    return conn

def run_git(args, cwd):
    flags = 0x08000000 if os.name == 'nt' else 0
    try:
        subprocess.run(['git'] + args, cwd=cwd, capture_output=True, check=False, creationflags=flags)
    except Exception:
        pass

def process_queue():
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("SELECT id, task_dir, files, commit_msg FROM tasks")
        rows = cursor.fetchall()

        if not rows:
            conn.close()
            return False

        for row in rows:
            row_id, task_dir, files_json, commit_msg = row
            try:
                files = json.loads(files_json)
            except Exception:
                cursor.execute("DELETE FROM tasks WHERE id=?", (row_id,))
                conn.commit()
                continue

            # Resolve current paths for each file
            resolved_moves = []
            for f in files:
                base_name = f['base']
                target_name = f['target']
                target_path = os.path.join(task_dir, target_name)
                current_path = None

                if os.path.exists(target_path):
                    current_path = target_path
                else:
                    for prefix in ['', '+', '-']:
                        p = os.path.join(task_dir, prefix + base_name)
                        if os.path.exists(p):
                            current_path = p
                            break

                if not current_path:
                    continue

                resolved_moves.append((current_path, target_path, base_name, target_name))

            if not resolved_moves:
                cursor.execute("DELETE FROM tasks WHERE id=?", (row_id,))
                conn.commit()
                continue

            # Try each file independently — не ждём закрытия всех, только заблокированных
            succeeded = []
            failed_files = []

            for src, dst, base_name, target_name in resolved_moves:
                if src == dst:
                    succeeded.append(dst)
                    continue

                try:
                    if os.path.exists(dst):
                        try:
                            os.remove(dst)
                        except Exception:
                            pass
                    os.rename(src, dst)
                    succeeded.append(dst)
                except OSError:
                    # Файл заблокирован — оставляем только его в очереди
                    failed_files.append({'base': base_name, 'target': target_name})
                except Exception:
                    failed_files.append({'base': base_name, 'target': target_name})

            # Git только для успешно переименованных
            if succeeded:
                for path in succeeded:
                    run_git(['add', path], repo_root())
                run_git(['commit', '-m', commit_msg], repo_root())

            # Обновляем очередь: удаляем задачу или оставляем только заблокированные файлы
            if not failed_files:
                cursor.execute("DELETE FROM tasks WHERE id=?", (row_id,))
            else:
                cursor.execute("UPDATE tasks SET files=? WHERE id=?",
                               (json.dumps(failed_files), row_id))
            conn.commit()

        conn.close()
        return True
    except Exception:
        return False

def main():
    f_lock = open(LOCK_FILE, 'w')
    try:
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(f_lock.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.lockf(f_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        return

    idle_count = 0
    while True:
        worked = process_queue()

        if worked:
            idle_count = 0
            time.sleep(0.5)
        else:
            idle_count += 1
            time.sleep(1)

        if idle_count > 15:
            break

    try:
        f_lock.close()
        os.remove(LOCK_FILE)
    except Exception:
        pass

if __name__ == "__main__":
    main()
