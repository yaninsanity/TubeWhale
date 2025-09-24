import os
import json
import datetime
import uuid
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Job status constants
JOB_STATUS_PENDING = 'pending'
JOB_STATUS_RUNNING = 'running'
JOB_STATUS_COMPLETED = 'completed'
JOB_STATUS_FAILED = 'failed'
JOB_STATUS_CANCELLED = 'cancelled'

VALID_STATUSES = [
    JOB_STATUS_PENDING,
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
    JOB_STATUS_CANCELLED
]


def _repo_root():
    # apps/client_app/job_store.py -> project root is three levels up
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def jobs_base_dir():
    root = _repo_root()
    out = os.path.join(root, 'outputs', 'jobs')
    os.makedirs(out, exist_ok=True)
    return out


def job_file_path(job_id: str) -> str:
    return os.path.join(jobs_base_dir(), f"{job_id}.json")


def create_job_record(job_info: dict) -> dict:
    """Create and persist a job record to outputs/jobs/<job_id>.json

    Returns the job record (with created_at and file path).
    """
    job_id = job_info.get('job_id')
    if not job_id:
        raise ValueError('job_info must include job_id')

    record = dict(job_info)
    record.setdefault('status', 'running')
    record.setdefault('progress', 0)
    record['created_at'] = datetime.datetime.utcnow().isoformat() + 'Z'
    path = job_file_path(job_id)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

    record['_file'] = path
    return record


def update_job_status(job_id: str, **updates) -> dict:
    """Update an existing job record with the provided keys."""
    path = job_file_path(job_id)
    if not os.path.exists(path):
        raise FileNotFoundError(f'Job record not found: {job_id}')

    with open(path, 'r', encoding='utf-8') as f:
        record = json.load(f)

    record.update(updates)
    record['last_updated'] = datetime.datetime.utcnow().isoformat() + 'Z'

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

    record['_file'] = path
    return record


def get_job_record(job_id: str) -> dict | None:
    path = job_file_path(job_id)
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        record = json.load(f)
    record['_file'] = path
    return record


def list_user_jobs(user_id: int) -> list:
    """Return a list of job records for a given user id."""
    jobs = []
    base = jobs_base_dir()
    for fname in os.listdir(base):
        if not fname.endswith('.json'):
            continue
        try:
            with open(os.path.join(base, fname), 'r', encoding='utf-8') as f:
                rec = json.load(f)
            if int(rec.get('user_id', -1)) == int(user_id):
                rec['_file'] = os.path.join(base, fname)
                jobs.append(rec)
        except Exception:
            # skip malformed files
            continue
    # newest first
    jobs.sort(key=lambda r: r.get('created_at', ''), reverse=True)
    return jobs


def create_job_record_v2(
    user_id: int,
    job_type: str,
    template_id: str,
    template_name: str,
    engine_config: Dict[str, Any],
    video_ids: Optional[List[str]] = None,
    playlist_id: Optional[str] = None,
    custom_prompts: Optional[Dict[str, Any]] = None,
    estimated_duration: int = 300
) -> str:
    """
    Enhanced job creation with comprehensive metadata
    
    Args:
        user_id: User ID who created the job
        job_type: 'single_video', 'playlist', or 'keyword_search'
        template_id: Selected template ID
        template_name: Human-readable template name
        engine_config: Engine configuration parameters
        video_ids: List of video IDs (for single video or playlist)
        playlist_id: YouTube playlist ID (if applicable)
        custom_prompts: Custom prompts if any
        estimated_duration: Estimated duration in seconds
    
    Returns:
        job_id: Unique job identifier
    """
    job_id = str(uuid.uuid4())
    timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
    
    job_data = {
        'job_id': job_id,
        'user_id': user_id,
        'job_type': job_type,
        'status': JOB_STATUS_PENDING,
        'created_at': timestamp,
        'last_updated': timestamp,
        'started_at': None,
        'completed_at': None,
        'template': {
            'template_id': template_id,
            'name': template_name
        },
        'engine_config': engine_config,
        'inputs': {
            'video_ids': video_ids or [],
            'playlist_id': playlist_id,
            'keyword': engine_config.get('keyword', ''),
            'custom_prompts': custom_prompts
        },
        'progress': {
            'percentage': 0,
            'current_step': 'Initializing...',
            'steps_completed': 0,
            'total_steps': estimate_total_steps(job_type, video_ids),
            'estimated_duration': estimated_duration,
            'time_elapsed': 0
        },
        'results': {
            'output_files': [],
            'summary': None,
            'statistics': {},
            'download_url': None
        },
        'process_info': {
            'pid': None,
            'log_file': None,
            'config_file': None
        },
        'error_info': {
            'error_message': None,
            'error_details': None,
            'retry_count': 0
        },
        'metadata': {
            'user_agent': 'TubeWhale-Client',
            'version': '1.0.0',
            'tags': generate_job_tags(job_type, template_id)
        }
    }
    
    try:
        path = job_file_path(job_id)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(job_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Created job record: {job_id} for user {user_id}")
        return job_id
    
    except Exception as e:
        logger.error(f"Failed to create job record {job_id}: {str(e)}")
        raise


def update_job_status_v2(
    job_id: str,
    status: str,
    progress_percentage: Optional[int] = None,
    current_step: Optional[str] = None,
    steps_completed: Optional[int] = None,
    error_message: Optional[str] = None,
    process_pid: Optional[int] = None,
    output_files: Optional[List[str]] = None,
    summary: Optional[str] = None
) -> bool:
    """Enhanced job status update with validation"""
    if status not in VALID_STATUSES:
        logger.error(f"Invalid status: {status}")
        return False
    
    try:
        job_data = get_job_record(job_id)
        if not job_data:
            logger.error(f"Job not found: {job_id}")
            return False
        
        # Update basic status
        job_data['status'] = status
        job_data['last_updated'] = datetime.datetime.utcnow().isoformat() + 'Z'
        
        # Update timing
        if status == JOB_STATUS_RUNNING and job_data.get('started_at') is None:
            job_data['started_at'] = datetime.datetime.utcnow().isoformat() + 'Z'
        elif status in [JOB_STATUS_COMPLETED, JOB_STATUS_FAILED, JOB_STATUS_CANCELLED]:
            job_data['completed_at'] = datetime.datetime.utcnow().isoformat() + 'Z'
        
        # Update progress
        if progress_percentage is not None:
            job_data['progress']['percentage'] = max(0, min(100, progress_percentage))
        
        if current_step is not None:
            job_data['progress']['current_step'] = current_step
        
        if steps_completed is not None:
            job_data['progress']['steps_completed'] = steps_completed
        
        # Update process info
        if process_pid is not None:
            job_data['process_info']['pid'] = process_pid
        
        # Update results
        if output_files is not None:
            job_data['results']['output_files'] = output_files
        
        if summary is not None:
            job_data['results']['summary'] = summary
        
        # Update error info
        if error_message is not None:
            job_data['error_info']['error_message'] = error_message
            job_data['error_info']['retry_count'] = job_data['error_info'].get('retry_count', 0) + 1
        
        # Save updated data
        path = job_file_path(job_id)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(job_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Updated job {job_id} status to {status}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to update job {job_id}: {str(e)}")
        return False


def get_job_summary_for_user(user_id: int) -> Dict[str, Any]:
    """Get summary statistics for user's jobs"""
    try:
        jobs = list_user_jobs(user_id)
        
        total_jobs = len(jobs)
        status_counts = {}
        type_counts = {}
        total_videos_processed = 0
        
        for job in jobs:
            # Count by status
            status = job.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Count by type
            job_type = job.get('job_type', 'unknown')
            type_counts[job_type] = type_counts.get(job_type, 0) + 1
            
            # Count videos processed
            video_ids = job.get('inputs', {}).get('video_ids', [])
            total_videos_processed += len(video_ids)
        
        return {
            'total_jobs': total_jobs,
            'status_distribution': status_counts,
            'type_distribution': type_counts,
            'total_videos_processed': total_videos_processed,
            'last_job_date': jobs[0].get('created_at') if jobs else None
        }
    
    except Exception as e:
        logger.error(f"Failed to get job summary for user {user_id}: {str(e)}")
        return {'error': str(e)}


def estimate_total_steps(job_type: str, video_ids: Optional[List[str]] = None) -> int:
    """Estimate total steps for progress tracking"""
    if job_type == 'single_video':
        return 5  # Download, transcribe, analyze, summarize, save
    elif job_type == 'playlist':
        video_count = len(video_ids) if video_ids else 10
        return video_count * 5 + 2  # Per video steps + playlist analysis + final summary
    elif job_type == 'keyword_search':
        return 8  # Search, filter, download, transcribe, analyze, summarize, save, finalize
    else:
        return 5  # Default


def generate_job_tags(job_type: str, template_id: str) -> List[str]:
    """Generate tags for job categorization"""
    tags = [job_type]
    
    if 'viral' in template_id:
        tags.append('viral-analysis')
    if 'content' in template_id:
        tags.append('content-strategy')
    if 'market' in template_id:
        tags.append('market-research')
    if 'academic' in template_id:
        tags.append('academic')
    
    return tags


def extract_video_id_from_url(url: str) -> Optional[str]:
    """Extract video ID from YouTube URL"""
    import re
    
    # YouTube video URL patterns
    patterns = [
        r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com\/v\/([a-zA-Z0-9_-]{11})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    # If it's already just a video ID
    if re.match(r'^[a-zA-Z0-9_-]{11}$', url):
        return url
    
    return None


def validate_job_inputs(job_type: str, **kwargs) -> tuple[bool, str]:
    """Validate job inputs based on job type"""
    if job_type == 'single_video':
        video_id = kwargs.get('video_id')
        video_url = kwargs.get('video_url')
        
        if not video_id and not video_url:
            return False, "Either video_id or video_url is required for single video jobs"
        
        if video_url and not extract_video_id_from_url(video_url):
            return False, "Invalid YouTube video URL format"
    
    elif job_type == 'playlist':
        playlist_id = kwargs.get('playlist_id')
        video_ids = kwargs.get('video_ids')
        
        if not playlist_id and not video_ids:
            return False, "Either playlist_id or video_ids list is required for playlist jobs"
        
        if video_ids and not isinstance(video_ids, list):
            return False, "video_ids must be a list"
        
        if video_ids and len(video_ids) == 0:
            return False, "video_ids list cannot be empty"
    
    elif job_type == 'keyword_search':
        keyword = kwargs.get('keyword')
        if not keyword or not keyword.strip():
            return False, "keyword is required for keyword search jobs"
    
    else:
        return False, f"Unknown job type: {job_type}"
    
    return True, "Valid inputs"
