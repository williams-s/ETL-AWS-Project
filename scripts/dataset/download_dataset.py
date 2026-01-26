import kagglehub
import os
import shutil

def download_netflix_data():

    mandatory_files = ['combined_data_1.txt', 'combined_data_2.txt', 'combined_data_3.txt', 'combined_data_4.txt', 'movie_titles.csv']
    needs_to_be_downloaded = False
    if os.path.exists("../data_raw"):
        for file in mandatory_files:
            if os.path.exists(f"../data_raw/{file}"):
                continue
            else:
                needs_to_be_downloaded = True
                break
    else:
        needs_to_be_downloaded = True

    if not needs_to_be_downloaded:
        print("Dataset Netflix deja telechargé !")
        return

    print("Téléchargement dataset Netflix...")
    
    download_path = kagglehub.dataset_download("netflix-inc/netflix-prize-data")
    print(f"Source: {download_path}")
    
    target_dir = "../data_raw"
    os.makedirs(target_dir, exist_ok=True)
    
    for file in os.listdir(download_path):
        src = os.path.join(download_path, file)
        dst = os.path.join(target_dir, file)
        
        if file.endswith('.txt') and 'combined_data' not in file:
            continue  
        
        shutil.copy2(src, dst)
    
    print("Dataset Netflix telechargé !")
        
if __name__ == "__main__":
    download_netflix_data()