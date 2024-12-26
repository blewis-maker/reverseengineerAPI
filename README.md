# reverseengineerAPI
KatapultManualAPIPull
# Katapult Automation Script

A Python automation tool that interfaces with KatapultPro's API to extract job data, process pole and anchor information, and generate reports. The script currently supports data extraction and local file generation, with email notifications and ArcGIS Enterprise updates planned for future implementation.

## Features

- Retrieves job data from KatapultPro API
- Processes poles, anchors, and connections data
- Generates GeoPackage files with spatial data
- Creates detailed Excel reports with job statistics
- Includes logging for tracking execution

## Prerequisites

- Python 3.9 or higher
- ArcGIS Pro installation
- Python packages listed in requirements.txt

## Installation

There are two ways to set up your Python environment:

### Option 1: Use Existing ArcGIS Pro Environment (Recommended)

1. Open ArcGIS Pro Python Command Prompt
2. Install required packages in the ArcGIS Pro environment:   ```bash
   pip install geopandas shapely pandas openpyxl   ```

### Option 2: Create New Environment with ArcGIS Access

1. Create a new conda environment that includes ArcGIS Pro packages:   ```bash
   conda create --name katapult_env --clone arcgispro-py3
   conda activate katapult_env
   pip install -r requirements.txt   ```

## Configuration

1. Update the workspace path in `main.py`:   ```python
   CONFIG = {
       # ... existing config ...
       'WORKSPACE_PATH': r"C:\Your\Path\Here\workspace"
   }   ```

2. Create the workspace directory if it doesn't exist.

## Usage

1. Basic usage:   ```bash
   python main.py   ```

2. To test a specific job:   ```python
   # In main.py, modify:
   TEST_ONLY_SPECIFIC_JOB = True
   TEST_JOB_ID = "your-job-id"   ```

## Output Files

The script generates several types of output:

1. **GeoPackage Files** (`workspace/Master.gpkg`):
   - Contains poles, anchors, and connections layers
   - Generated in your specified workspace directory

2. **Excel Reports** (`workspace/Aerial_Status_Report_[timestamp].xlsx`):
   - Job status summaries
   - MR status counts
   - Overall project statistics

3. **Log File** (`katapult_automation.log`):
   - Detailed execution logs
   - Error tracking
   - Processing statistics

## Known Limitations

1. **Feature Service Updates**: Currently not functional. You may see errors related to ArcGIS Enterprise updates - these can be ignored.

2. **Email Notifications**: Currently not implemented. Email-related errors in the logs can be ignored.

## Troubleshooting

### Common Issues

1. **ArcGIS Module Import Error**   ```
   ModuleNotFoundError: No module named 'arcgis'   ```
   Solution: Ensure you're using the correct Python environment with ArcGIS Pro packages

2. **Workspace Path Errors**   ```
   FileNotFoundError: [Errno 2] No such file or directory   ```
   Solution: 
   - Verify your workspace path in the CONFIG
   - Ensure the directory exists
   - Check write permissions

3. **Package Import Errors**   ```
   ModuleNotFoundError: No module named 'package_name'   ```
   Solution: Install missing packages using pip in your active environment

## Logging

The script logs all operations to `katapult_automation.log`. Check this file for execution details and any errors.

## Future Implementations

- ArcGIS Enterprise feature layer updates
- Email notifications
- Additional reporting features

## Support

For support, please:
1. Check the troubleshooting section
2. Review existing issues
3. Create a new issue with:
   - Detailed description of the problem
   - Relevant log entries
   - Steps to reproduce
