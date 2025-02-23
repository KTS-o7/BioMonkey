import streamlit as st
import subprocess
import json
import time
import re
from pathlib import Path

def parse_log_line(line):
    """Parse log lines to extract metrics"""
    if "Clean datasets collected:" in line:
        match = re.search(r"Clean datasets collected: (\d+)/(\d+)", line)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None

def get_current_log_file():
    """Get the most recent log file"""
    log_files = list(Path('.').glob('sra_downloader_*.log'))
    if log_files:
        return max(log_files, key=lambda x: x.stat().st_mtime)
    return None

def create_metrics_container():
    """Create and return containers for metrics"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        datasets_checked = st.metric(
            label="Datasets Checked",
            value="0"
        )
    
    with col2:
        clean_datasets = st.metric(
            label="Clean Datasets",
            value="0"
        )
    
    with col3:
        success_rate = st.metric(
            label="Success Rate",
            value="0%"
        )
        
    return datasets_checked, clean_datasets, success_rate

def create_status_area():
    """Create containers for status updates"""
    # Create two columns - one for current status, one for recent events
    status_col, events_col = st.columns([1, 2])
    
    with status_col:
        st.subheader("Current Status")
        status_container = st.empty()
        
    with events_col:
        st.subheader("Recent Events")
        events_container = st.empty()
        
    # Add custom CSS for events container
    st.markdown("""
        <style>
        .event-box {
            padding: 8px 12px;
            margin: 8px 0;
            background-color: var(--st-color-white);
            border-radius: var(--st-radius-md);
            font-size: 14px;
            border-left: 4px solid var(--st-color-secondary);
        }
        .event-box.download { border-left-color: var(--st-color-primary); }
        .event-box.analysis { border-left-color: var(--st-color-secondary); }
        .event-box.fail { border-left-color: var(--st-color-error); }
        .event-box.pass { border-left-color: var(--st-color-success); }
        .event-box.progress { border-left-color: var(--st-color-warning); }
        .events-container {
            max-height: 300px;
            overflow-y: auto;
            padding: 10px;
            border: 1px solid var(--st-color-border);
            border-radius: var(--st-radius-lg);
            background-color: var(--st-color-background);
        }
        </style>
    """, unsafe_allow_html=True)
        
    return status_container, events_container

def parse_status_from_log(line):
    """Parse a log line and return a formatted status message"""
    # Extract timestamp if present
    timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\w+) - (.+)', line)
    if timestamp_match:
        message = timestamp_match.group(3)
    else:
        message = line

    if "Starting download process for term:" in message:
        return f"🚀 Starting download process...\n {message}", "start"
    elif "Searching SRA database" in message:
        return f"🔍 Searching SRA database...\n {message}", "search"
    elif "Run info list:" in message:
        return "📋 Retrieved dataset information", "info"
    elif "Downloading" in message and ".fastq.gz" in message:
        match = re.search(r'Downloading (.+\.fastq\.gz)', message)
        filename = match.group(1) if match else "dataset"
        return f"⬇️ Downloading {filename}", "download"
    elif "Running FastQC:" in message:
        return "🔬 Running quality check...", "analysis"
    elif "FastQC found quality issues" in message:
        return "❌ Dataset failed quality check", "fail"
    elif "File passed quality check" in message:
        return "✅ Dataset passed quality check", "pass"
    elif "Clean datasets collected:" in message:
        match = re.search(r"Clean datasets collected: (\d+)/(\d+)", message)
        if match:
            current, target = match.groups()
            return f"📊 Progress: {current}/{target} clean datasets", "progress"
    elif "Process completed" in message:
        return "✨ Process completed", "complete"
    return None, None

def main():
    st.set_page_config(
        page_title="BioMonkey - SRA Dataset Downloader",
        page_icon="🧬",
        layout="wide"
    )
    
    # Header
    st.title("🧬 BioMonkey - SRA Dataset Downloader")
    st.markdown("""
    Download and quality check SRA datasets in parallel with automated FastQC analysis.
    """)
    
    # Sidebar for inputs
    with st.sidebar:
        st.header("Download Parameters")
        term = st.text_input(
            "Search Term",
            placeholder="e.g., cancer, COVID-19, etc.",
            help="Enter a search term to query the NCBI SRA database"
        )
        
        num_datasets = st.number_input(
            "Number of Clean Datasets",
            min_value=1,
            max_value=100,
            value=10,
            help="Number of quality-checked datasets to download"
        )
        
        num_workers = st.slider(
            "Number of Parallel Downloads",
            min_value=1,
            max_value=8,
            value=3,
            help="Number of simultaneous downloads. Recommended: 3-5 for average networks"
        )
        
        # Add network speed recommendation
        if num_workers <= 2:
            st.info("💡 Recommended for slower networks")
        elif num_workers <= 5:
            st.success("💡 Recommended for average networks")
        else:
            st.warning("💡 Only use high parallelism with fast networks")
        
        start_button = st.button(
            "Start Download",
            use_container_width=True,
            disabled=not term
        )
    
    # Main content area
    if not start_button:
        # Show welcome message when not running
        st.info("Configure your download parameters in the sidebar and click 'Start Download' to begin.")
        return
        
    # Create status areas
    status_container, events_container = create_status_area()
    
    # Create metrics containers
    datasets_checked, clean_datasets, success_rate = create_metrics_container()
    
    # Progress bar with custom styling
    st.markdown("""
        <style>
            .stProgress > div > div > div > div {
                background-color: #00cc00;
            }
        </style>""", 
        unsafe_allow_html=True
    )
    progress_bar = st.progress(0)
    
    # Initialize event log
    recent_events = []
    max_events = 5  # Number of recent events to show
    
    # Start the download process
    command = f"python3 sra_downloader.py --term '{term}' --num_datasets {num_datasets} --workers {num_workers}"
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1
    )
    
    # Initialize counters
    total_checked = 0
    current_clean = 0
    current_status = "🚀 Starting process..."
    
    while True:
        log_file = get_current_log_file()
        if log_file:
            with open(log_file, 'r') as f:
                log_content = f.read()
                
                # Update metrics and status
                for line in log_content.split('\n'):
                    status_msg, status_type = parse_status_from_log(line)
                    
                    if status_msg:
                        current_status = status_msg
                        # Add timestamp to events
                        timestamp = time.strftime("%H:%M:%S")
                        event = f"{timestamp} - {status_msg}"
                        
                        if event not in recent_events:
                            recent_events.insert(0, event)
                            recent_events = recent_events[:max_events]
                    
                    if "FastQC found quality issues" in line:
                        total_checked += 1
                    elif "File passed quality check" in line:
                        total_checked += 1
                        current_clean += 1
                
                # Update status display
                status_container.markdown(f"""
                ### {current_status}
                """)
                
                # Update events display
                events_html = "<div class='events-container'>"
                for event in recent_events:
                    # Extract status type from the event message
                    _, status_type = parse_status_from_log(event)
                    status_class = f"event-box {status_type}" if status_type else "event-box"
                    events_html += f"<div class='{status_class}'>{event}</div>"
                events_html += "</div>"
                events_container.markdown(events_html, unsafe_allow_html=True)
                
                # Update progress and metrics
                if current_clean > 0:
                    progress = min(current_clean / num_datasets, 1.0)
                    progress_bar.progress(progress)
                    
                datasets_checked.metric(
                    label="Datasets Checked",
                    value=str(total_checked)
                )
                clean_datasets.metric(
                    label="Clean Datasets",
                    value=f"{current_clean}/{num_datasets}"
                )
                if total_checked > 0:
                    rate = (current_clean / total_checked) * 100
                    success_rate.metric(
                        label="Success Rate",
                        value=f"{rate:.1f}%"
                    )
        
        # Check if process has completed
        if process.poll() is not None:
            break
            
        time.sleep(1)
    
    # Final status update
    if current_clean >= num_datasets:
        status_container.success("✨ Process completed successfully!")
        st.balloons()
    else:
        status_container.warning("⚠️ Process completed with fewer datasets than requested")

if __name__ == "__main__":
    main()