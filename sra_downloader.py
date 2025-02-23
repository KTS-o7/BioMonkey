import os
import argparse
import logging
from dotenv import load_dotenv
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed



# Set up logging
log_filename = f"sra_downloader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

# Get and validate environment variables
email = os.getenv("NCBI_EMAIL")
api_key = os.getenv("NCBI_API_KEY")
fastqc_path = os.getenv("FASTQC_PATH")
NCBI_EUTILS_BASEURL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
NCBI_TRACE_BASEURL = "https://trace.ncbi.nlm.nih.gov/Traces/sra-reads-be/fastq?acc="
GZ_TEMP_FOLDER = "temp_gz_files"
FASTQ_TEMP_FOLDER = "fastqc_temp"
CLEAN_DATASET_FOLDER = "clean_datasets"

if not all([email, api_key, fastqc_path]):
    logging.error("Missing required environment variables. Please check .env file")
    raise ValueError("NCBI_EMAIL, NCBI_API_KEY, and FASTQC_PATH must be set in .env file")

def get_uid_from_term(term:str,retmax:int=10,retstart:int=0) -> list[int]:
    """
    Based on the search term ping the NCBI SRA Database and return a list of 
    """
    url = f"{NCBI_EUTILS_BASEURL}esearch.fcgi?db=sra&term={term}&retmax={retmax}&retstart={retstart}"
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data from NCBI: {response.status_code}")
        raise Exception(f"Failed to fetch data from NCBI: {response.status_code}")
    
    # response of this url is a xml file like this:
    """
    <eSearchResult>
<Count>1186478</Count>
<RetMax>10</RetMax>
<RetStart>0</RetStart>
<IdList>
<Id>37400425</Id>
<Id>37400424</Id>
<Id>37400423</Id>
<Id>37400422</Id>
<Id>37400421</Id>
<Id>37400420</Id>
<Id>37400419</Id>
<Id>37400418</Id>
<Id>37400417</Id>
<Id>37400416</Id>
</IdList>
</eSearchResult>
    """
    # parse the xml file and return a list of uids
    root = ET.fromstring(response.text)
    maximum_count = int(root.find('.//Count').text)
    previous_start = retstart
    uids = [int(uid.text) for uid in root.findall('.//Id')]
    return uids,maximum_count,previous_start

def get_sraid_from_uid(uid_list:list[int]) -> list[str]:
    """
    Based on the list of uids ping the NCBI SRA Database and return a list of sra ids.
    This will return a XML file with parameters like
<Row>
<Run>SRR32410640</Run>
<ReleaseDate>2025-02-21 00:46:00</ReleaseDate>
<LoadDate>2025-02-20 06:53:09</LoadDate>
<spots>20562488</spots>
<bases>6168746400</bases>
<spots_with_mates>20562488</spots_with_mates>
<avgLength>300</avgLength>
<size_MB>1743</size_MB>
<download_path>https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos7/sra-pub-zq-41/SRR032/32410/SRR32410640/SRR32410640.lite.1</download_path>
<Experiment>SRX27741038</Experiment>
<LibraryName>GSM8804246</LibraryName>
<LibraryStrategy>ChIP-Seq</LibraryStrategy>
<LibrarySelection>ChIP</LibrarySelection>
<LibrarySource>GENOMIC</LibrarySource>
<LibraryLayout>PAIRED</LibraryLayout>
<InsertSize>0</InsertSize>
<InsertDev>0</InsertDev>
<Platform>ILLUMINA</Platform>
<Model>Illumina HiSeq 2000</Model>
<SRAStudy>SRP565197</SRAStudy>
<BioProject>PRJNA1225950</BioProject>
<ProjectID>1225950</ProjectID>
<Sample>SRS24131419</Sample>
<BioSample>SAMN46912170</BioSample>
<SampleType>simple</SampleType>
<TaxID>10090</TaxID>
<ScientificName>Mus musculus</ScientificName>
<SampleName>GSM8804246</SampleName>
<Tumor>no</Tumor>
<CenterName>TONGJI UNIVERSITY</CenterName>
<Submission>SRA2080621</Submission>
<Consent>public</Consent>
<RunHash>7CAAE5A018F9229A9DA808C82EC0FBB9</RunHash>
<ReadHash>69FE533F4AF949F478DA99A259F2458D</ReadHash>
</Row>
</SraRunInfo>
    """
    # From this XML file extract create a dict of each row and return a list of dicts
# sample url https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=sra&id=37384107,37380910&rettype=runinfo&retmode=xml
    
    url = f"{NCBI_EUTILS_BASEURL}efetch.fcgi?db=sra&id={','.join(map(str,uid_list))}&rettype=runinfo&retmode=xml"
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data from NCBI: {response.status_code}")
        raise Exception(f"Failed to fetch data from NCBI: {response.status_code}")
    
        # parse the xml file and return a list of dicts
    root = ET.fromstring(response.text)
    run_info_list = []
    for row in root.findall('Row'):
        run_info = {}
        for child in row:
            run_info[child.tag.lower()] = child.text
        run_info_list.append(run_info)
    #pretty print the run_info_list
    logging.info(f"Run info list: {json.dumps(run_info_list, indent=4)}")
    return run_info_list

def download_single_sra_file(run_info: dict, output_dir: str) -> tuple[str, bool]:
    """
    Download a single SRA file with progress tracking
    
    Returns:
        tuple[str, bool]: (sra_id, success_status)
    """
    try:
        sra_id = run_info['run']
        trace_url = f"{NCBI_TRACE_BASEURL}{sra_id}"
        sra_file_name = f"{sra_id}.fastq.gz"
        sra_file_path = os.path.join(output_dir, sra_file_name)
        
        # Skip if file already exists
        if os.path.exists(sra_file_path):
            logging.info(f"File {sra_file_name} already exists, skipping...")
            return sra_id, True
        
        headers = {
            'User-Agent': f'SRA Downloader (contact: {os.getenv("NCBI_EMAIL")})'
        }
        
        # Get file size
        response = requests.head(trace_url, headers=headers)
        total_size = int(response.headers.get('content-length', 0))
        
        # Download with progress bar
        with requests.get(trace_url, stream=True, headers=headers) as r:
            r.raise_for_status()
            
            with open(sra_file_path, 'wb') as f:
                with tqdm(
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    desc=f"Downloading {sra_file_name}",
                    leave=True
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        pbar.update(size)
        
        # Verify file size
        actual_size = os.path.getsize(sra_file_path)
        if actual_size < total_size:
            raise Exception(f"Downloaded file size ({actual_size}) is less than expected ({total_size})")
        
        logging.info(f"Successfully downloaded {sra_file_name}")
        return sra_id, True
        
    except Exception as e:
        logging.error(f"Failed to download {sra_id}: {str(e)}")
        # Clean up partial download if it exists
        if 'sra_file_path' in locals() and os.path.exists(sra_file_path):
            os.remove(sra_file_path)
        return sra_id, False

def process_downloaded_file(sra_id: str, gz_file_path: str, clean_dataset_dir: str) -> bool:
    """
    Process a single downloaded file with FastQC
    
    Returns:
        bool: True if file passes quality check and is moved to clean dataset folder
    """
    try:
        if not os.path.exists(gz_file_path):
            return False
            
        # Create temp output directory for FastQC
        temp_output_dir = os.path.dirname(gz_file_path)
            
        # Run FastQC and move to clean dataset if passes
        if run_fastqc_and_save_in_clean_dataset_folder_if_all_params_in_summary_txt_file_are_NOT_FAIL(
            gz_file_path, 
            temp_output_dir,
            clean_dataset_dir
        ):
            logging.info(f"File {sra_id} passed quality check")
            return True
            
    except Exception as e:
        logging.error(f"Error processing file {sra_id}: {str(e)}")
    finally:
        # Clean up the temporary gz file
        if os.path.exists(gz_file_path):
            os.remove(gz_file_path)
    
    return False

def download_and_process_parallel(run_info_list: list[dict], output_dir: str, clean_dataset_dir: str, max_workers: int = 5):
    """
    Download files in parallel and process them as they complete downloading
    """
    os.makedirs(output_dir, exist_ok=True)
    processed_files = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_sra = {
            executor.submit(download_single_sra_file, run_info, output_dir): run_info['run']
            for run_info in run_info_list
        }
        
        # Process files as they complete downloading
        for future in as_completed(future_to_sra):
            sra_id, success = future.result()
            if success:
                gz_file_path = os.path.join(output_dir, f"{sra_id}.fastq.gz")
                if process_downloaded_file(sra_id, gz_file_path, clean_dataset_dir):
                    processed_files.append(sra_id)
                    
    return processed_files

def run_fastqc_and_save_in_clean_dataset_folder_if_all_params_in_summary_txt_file_are_NOT_FAIL(
    fastq_gz_file_path: str,
    temp_output_dir: str,
    clean_dataset_dir: str
) -> bool:
    """
    Run FastQC on the given fastq.gz file and save the results in the clean dataset folder if all parameters in the summary.txt file are NOT FAIL
    
    Args:
        fastq_gz_file_path: Path to the input fastq.gz file
        temp_output_dir: Directory for temporary FastQC output
        clean_dataset_dir: Directory where clean datasets should be saved
    
    Returns:
        bool: True if file passes quality check and is copied to clean dataset folder
    """
    # Create FastQC output directory
    fastqc_output_dir = os.path.join(temp_output_dir, FASTQ_TEMP_FOLDER)
    os.makedirs(fastqc_output_dir, exist_ok=True)
    
    try:
        # Run FastQC command
        fastqc_cmd = f"{fastqc_path} {fastq_gz_file_path} -o {fastqc_output_dir} --extract"
        logging.info(f"Running FastQC: {fastqc_cmd}")
        os.system(fastqc_cmd)

        # Get the FastQC results directory name
        fastq_filename = os.path.basename(fastq_gz_file_path)
        fastqc_results_name = fastq_filename.replace('.fastq.gz', '_fastqc')
        fastqc_results_dir = os.path.join(fastqc_output_dir, fastqc_results_name)

        # Read the summary.txt file
        summary_file = os.path.join(fastqc_results_dir, 'summary.txt')
        if not os.path.exists(summary_file):
            logging.error(f"FastQC summary file not found: {summary_file}")
            return False

        # Check if any parameter is FAIL
        with open(summary_file, 'r') as f:
            for line in f:
                if 'FAIL' in line:
                    logging.info(f"FastQC found quality issues in {fastq_filename}: {line.strip()}")
                    return False

        # If no FAIL found, copy the file to clean dataset folder
        os.makedirs(clean_dataset_dir, exist_ok=True)
        clean_file_path = os.path.join(clean_dataset_dir, fastq_filename)
        
        # Use copy2 to preserve metadata
        from shutil import copy2
        copy2(fastq_gz_file_path, clean_file_path)
        logging.info(f"File passed quality check and copied to clean dataset folder: {clean_file_path}")
        
        return True

    except Exception as e:
        logging.error(f"Error running FastQC on {fastq_gz_file_path}: {str(e)}")
        return False
    finally:
        # Clean up temporary FastQC output
        import shutil
        if 'fastqc_results_dir' in locals() and os.path.exists(fastqc_results_dir):
            shutil.rmtree(fastqc_results_dir)

def main(search_term: str, num_clean_datasets: int, workers: int):
    logging.info(f"Starting download process for term: {search_term}, target: {num_clean_datasets} clean datasets")
    
    retstart = 0
    clean_datasets_count = 0
    batch_size = 10
    
    # Create necessary directories
    os.makedirs(GZ_TEMP_FOLDER, exist_ok=True)
    clean_dataset_dir = os.path.join(os.path.dirname(__file__), CLEAN_DATASET_FOLDER)
    
    while clean_datasets_count < num_clean_datasets:
        try:
            # Get UIDs for the current batch
            uids, max_count, _ = get_uid_from_term(search_term, retmax=batch_size, retstart=retstart)
            
            if not uids:
                logging.warning("No more datasets available from the search term")
                break
                
            # Get SRA run info for the UIDs
            run_info_list = get_sraid_from_uid(uids)
            
            # Download and process files in parallel
            processed_files = download_and_process_parallel(run_info_list, GZ_TEMP_FOLDER, CLEAN_DATASET_FOLDER, max_workers=workers)
            
            clean_datasets_count += len(processed_files)
            logging.info(f"Clean datasets collected: {clean_datasets_count}/{num_clean_datasets}")
            
            if clean_datasets_count >= num_clean_datasets:
                break
            
            retstart += batch_size
            
            # Check if we've processed all available datasets
            if retstart >= max_count:
                logging.warning("Reached the end of available datasets")
                break
                
        except Exception as e:
            logging.error(f"Error in main processing loop: {str(e)}")
            continue
    
    # Clean up temporary folders
    #if os.path.exists(GZ_TEMP_FOLDER):
        #import shutil
        #shutil.rmtree(GZ_TEMP_FOLDER)
    
    logging.info(f"Process completed. Collected {clean_datasets_count} clean datasets")
    if clean_datasets_count < num_clean_datasets:
        logging.warning(f"Could only collect {clean_datasets_count} clean datasets out of requested {num_clean_datasets}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SRA Dataset Downloader")
    parser.add_argument("--term", required=True, help="Search term for SRA database")
    parser.add_argument("--num_datasets", type=int, required=True, help="Number of clean datasets needed")
    parser.add_argument("--workers", type=int, default=3, help="Number of parallel downloads (1-8, recommended: 3-5)")
    args = parser.parse_args()
    
    # Validate workers argument
    workers = max(1, min(8, args.workers))
    
    main(args.term, args.num_datasets, workers)