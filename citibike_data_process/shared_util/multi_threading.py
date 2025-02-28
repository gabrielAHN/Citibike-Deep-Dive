from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def parallel_execute(function, data_list, workers):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(function, data_list))

def parallel_file_upload(function, data_list, workers, show_progress=False):
    """Executes function in parallel and tracks progress of each task."""
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(function, data): data for data in data_list}

        if show_progress:
            with tqdm(total=len(data_list), desc="Uploading Files", unit="file") as pbar:
                for future in as_completed(futures):
                    results.append(future.result())  # Collect results as they complete
                    pbar.update(1)  # Update progress bar
        else:
            for future in as_completed(futures):
                results.append(future.result())

    return results 