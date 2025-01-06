import pandas as pd
import os
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import logging

# Configure logger
logger = logging.getLogger(__name__)

class WeeklyReportGenerator:
    def __init__(self, metrics, report_date=None):
        self.metrics = metrics
        self.current_row = 1  # Initialize current_row
        # Ensure report_date is a datetime object
        if isinstance(report_date, str):
            try:
                self.report_date = datetime.strptime(report_date, '%Y-%m-%d')
            except ValueError:
                self.report_date = datetime.now()
        elif isinstance(report_date, datetime):
            self.report_date = report_date
        else:
            self.report_date = datetime.now()
            
        # Adjust report_date to Sunday 8:00 AM if not already
        days_until_sunday = (6 - self.report_date.weekday()) % 7
        self.report_date = self.report_date + timedelta(days=days_until_sunday)
        self.report_date = self.report_date.replace(hour=8, minute=0, second=0, microsecond=0)
        
        logger.info(f"Initializing Weekly Report Generator for date: {self.report_date}")
        self.wb = Workbook()
        
    def generate_report(self, output_path):
        """Generate the weekly report Excel file."""
        try:
            logger.info(f"Starting weekly report generation at {output_path}")
            logger.info(f"Report period: {(self.report_date - timedelta(days=7)).strftime('%Y-%m-%d')} to {self.report_date.strftime('%Y-%m-%d')}")
            
            # Create sheets
            weekly_metrics = self.wb.active
            weekly_metrics.title = "Weekly Status"
            burndown = self.wb.create_sheet("Burndown")
            schedule = self.wb.create_sheet("Schedule")
            charts = self.wb.create_sheet("Charts")
            
            logger.info("Created worksheet structure")
            
            # Generate each section
            logger.info("Generating report sections...")
            self._generate_header(weekly_metrics)
            
            # Generate and log utility progress
            logger.info("Generating Utility Progress section")
            utility_metrics = self.metrics.get_weekly_status()['utility_metrics']
            logger.info(f"Processing metrics for {len(utility_metrics)} utilities:")
            for utility, metrics in utility_metrics.items():
                logger.info(f"  {utility}:")
                logger.info(f"    Total Poles: {metrics['total_poles']}")
                logger.info(f"    Completed Poles: {metrics['completed_poles']}")
                logger.info(f"    Run Rate: {metrics['run_rate']:.1f} poles/week")
                if metrics.get('estimated_completion'):
                    logger.info(f"    Est. Completion: {metrics['estimated_completion']}")
            weekly_status = self.metrics.get_weekly_status()
            self._generate_utility_progress(weekly_metrics)
            
            # Generate and log OSP productivity
            logger.info("Generating OSP Productivity section")
            user_metrics = self.metrics.get_weekly_status()['user_production']
            for category in ['field', 'annotation', 'sent_to_pe', 'delivery', 'emr', 'approved']:
                logger.info(f"  {category.title()} Users:")
                users = user_metrics.get(category, [])
                for user_data in users:
                    logger.info(f"    {user_data.get('user', 'Unknown')}:")
                    logger.info(f"      Completed Poles: {user_data.get('completed_poles', user_data.get('pole_count', 0))}")
                    logger.info(f"      Utilities: {', '.join(user_data.get('utilities', []))}")
            self._generate_osp_productivity(weekly_metrics)
            
            # Generate and log status tracking
            logger.info("Generating Status Tracking section")
            status_changes = self.metrics.get_weekly_status()['status_changes']
            for status, changes in status_changes.items():
                logger.info(f"  {status}: {len(changes)} changes")
                for change in changes:
                    logger.info(f"    Job {change['job_id']}: {change['pole_count']} poles, {change['utility']}, {change['date']}")
            self._generate_status_tracking(weekly_metrics, weekly_status)
            
            # Generate and log burndown metrics
            logger.info("Generating Burndown Metrics section")
            burndown_metrics = self.metrics.get_weekly_status()['burndown']
            logger.info("Utility Burndown:")
            for utility, metrics in burndown_metrics['by_utility'].items():
                logger.info(f"  {utility}:")
                logger.info(f"    Total Poles: {metrics['total_poles']}")
                logger.info(f"    Completed: {metrics['completed_poles']}")
                logger.info(f"    Run Rate: {metrics['run_rate']:.1f}")
                if metrics.get('estimated_completion'):
                    logger.info(f"    Est. Completion: {metrics['estimated_completion']}")
            
            logger.info("Backlog Analysis:")
            for category, stats in burndown_metrics['backlog'].items():
                logger.info(f"  {category}:")
                logger.info(f"    Total Poles: {stats['total_poles']}")
                logger.info(f"    Jobs: {len(stats['jobs'])}")
                logger.info(f"    Utilities: {len(stats['utilities'])}")
            self._generate_burndown_metrics(burndown, weekly_status)
            
            # Generate and log schedule metrics
            logger.info("Generating Schedule Metrics section")
            schedule_metrics = self.metrics.get_weekly_status()['schedule']
            logger.info(f"Processing {len(schedule_metrics['projects'])} projects:")
            for project in schedule_metrics['projects']:
                logger.info(f"  {project['project_id']}:")
                logger.info(f"    Total Poles: {project['total_poles']}")
                logger.info(f"    Completed: {project['completed_poles']}")
                progress = (project['completed_poles'] / project['total_poles'] * 100) if project['total_poles'] > 0 else 0
                logger.info(f"    Progress: {progress:.1f}%")
                logger.info(f"    Field Users: {project['field_users']}")
                logger.info(f"    Back Office Users: {project['back_office_users']}")
                logger.info(f"    End Date: {project.get('end_date', 'TBD')}")
            self._generate_schedule_metrics(schedule, weekly_status)
            
            # Generate charts
            self._generate_charts(charts)
            
            # Save the workbook
            self.wb.save(output_path)
            logger.info(f"Report saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            logger.error("Failed to generate report")
            return False
            
    def _generate_header(self, ws):
        """Generate the report header."""
        ws['A1'] = "OSP Production Master - Weekly Status Report"
        ws['A2'] = f"Generated: {self.report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        start_date = self.report_date - timedelta(days=7)
        ws['A3'] = f"Date Range: {start_date.strftime('%Y-%m-%d')} to {self.report_date.strftime('%Y-%m-%d')}"
        
        # Style header
        for cell in ['A1', 'A2', 'A3']:
            ws[cell].font = Font(bold=True)
            
    def _generate_status_tracking(self, worksheet, weekly_status):
        """Generate status tracking section of the report."""
        logging.info("Generating status tracking section")
        
        # Set up headers
        headers = ['Status', 'Job ID', 'Utility', 'Pole Count', 'Date']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add status changes
        total_changes = 0
        for status, changes in weekly_status['status_changes'].items():
            for change in changes:
                row = [
                    status,
                    change['job_id'],
                    change['utility'],
                    change['pole_count'],
                    change['date'].strftime('%Y-%m-%d') if isinstance(change['date'], datetime) else change['date']
                ]
                for col, value in enumerate(row):
                    cell = worksheet.cell(row=self.current_row, column=col + 1)
                    cell.value = value
                self.current_row += 1
                total_changes += 1
                
        logging.info(f"Added {total_changes} status changes")
        
        # Add spacing
        self.current_row += 2

    def _generate_burndown_metrics(self, worksheet, weekly_status):
        """Generate burndown metrics section of the report."""
        logging.info("Generating burndown metrics section")
        
        # Set up headers for utility burndown
        headers = ['Utility', 'Total Poles', 'Completed Poles', 'Run Rate', 'Est. Completion']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add utility burndown metrics
        burndown = weekly_status['burndown']
        for utility, metrics in burndown['by_utility'].items():
            row = [
                utility,
                metrics['total_poles'],
                metrics['completed_poles'],
                f"{metrics['run_rate']:.1f} poles/week",
                metrics['estimated_completion'].strftime('%Y-%m-%d') if isinstance(metrics['estimated_completion'], datetime) else metrics['estimated_completion'] or 'TBD'
            ]
            for col, value in enumerate(row):
                cell = worksheet.cell(row=self.current_row, column=col + 1)
                cell.value = value
            self.current_row += 1
            
        # Add spacing
        self.current_row += 2
        
        # Add project burndown metrics header
        headers = ['Project', 'Total Poles', 'Completed Poles', 'Run Rate', 'Est. Completion']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add project burndown metrics
        for project, metrics in burndown['by_project'].items():
            row = [
                project,
                metrics['total_poles'],
                metrics['completed_poles'],
                f"{metrics['run_rate']:.1f} poles/week",
                metrics['estimated_completion'].strftime('%Y-%m-%d') if isinstance(metrics['estimated_completion'], datetime) else metrics['estimated_completion'] or 'TBD'
            ]
            for col, value in enumerate(row):
                cell = worksheet.cell(row=self.current_row, column=col + 1)
                cell.value = value
            self.current_row += 1
            
        # Add spacing
        self.current_row += 2
        
        # Add backlog analysis header
        worksheet.cell(row=self.current_row, column=1).value = "Backlog Analysis"
        worksheet.cell(row=self.current_row, column=1).font = Font(bold=True)
        self.current_row += 1
        
        # Set up headers for backlog
        headers = ['Category', 'Total Poles', 'Jobs', 'Utilities']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add backlog metrics
        backlog = burndown['backlog']
        categories = {
            'field': 'Field Collection',
            'back_office': 'Back Office',
            'approve_construction': 'Approve for Construction'
        }
        
        for category, display_name in categories.items():
            if category in backlog:
                metrics = backlog[category]
                row = [
                    display_name,
                    metrics['total_poles'],
                    len(metrics['jobs']),
                    len(metrics['utilities'])
                ]
                for col, value in enumerate(row):
                    cell = worksheet.cell(row=self.current_row, column=col + 1)
                    cell.value = value
                self.current_row += 1
        
        logging.info("Added burndown metrics")
        
        # Add spacing
        self.current_row += 2

    def _generate_schedule_metrics(self, worksheet, weekly_status):
        """Generate schedule metrics section of the report."""
        logging.info("Generating schedule metrics section")
        
        # Set up headers
        headers = ['Project', 'Total Poles', 'Completed Poles', 'Progress', 'Field Users', 'Back Office Users', 'End Date']
        for col, header in enumerate(headers):
            cell = worksheet.cell(row=self.current_row, column=col + 1)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
            
        self.current_row += 1
        
        # Add project metrics
        schedule = weekly_status['schedule']
        for project in schedule['projects']:
            progress = (project['completed_poles'] / project['total_poles'] * 100) if project['total_poles'] > 0 else 0
            row = [
                project['project_id'],
                project['total_poles'],
                project['completed_poles'],
                f"{progress:.1f}%",
                len(project['field_users']),
                len(project['back_office_users']),
                project.get('end_date', 'TBD')
            ]
            for col, value in enumerate(row):
                cell = worksheet.cell(row=self.current_row, column=col + 1)
                cell.value = value
            self.current_row += 1
            
        logging.info("Added schedule metrics")
        
        # Add spacing
        self.current_row += 2

    def _generate_utility_progress(self, ws):
        """Generate the Utility Progress section."""
        logging.info("Generating Utility Progress section")
        
        # Section header
        ws.cell(row=7, column=1).value = "Utility Progress"
        ws.cell(row=7, column=1).font = Font(bold=True)
        
        # Column headers
        headers = ['Utility', 'Total Poles', 'Completed', 'Remaining', 'Completion %', 'Est. Completion Date']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=8, column=col)
            cell.value = header
            cell.font = Font(bold=True)
        
        # Data rows
        row = 9
        utility_metrics = self.metrics.burndown['by_utility']
        logging.info(f"Processing utility metrics: {utility_metrics}")
        
        for utility, data in utility_metrics.items():
            logging.debug(f"Processing utility: {utility}")
            logging.debug(f"Utility data: {data}")
            
            total_poles = data.get('total_poles', 0)
            completed = data.get('completed_poles', 0)
            remaining = total_poles - completed
            completion_pct = (completed / total_poles * 100) if total_poles > 0 else 0
            est_completion = data.get('estimated_completion_date', 'N/A')
            
            logging.debug(f"Utility stats - Total: {total_poles}, Completed: {completed}, " 
                         f"Remaining: {remaining}, Completion %: {completion_pct:.1f}, "
                         f"Est. Completion: {est_completion}")
            
            ws.cell(row=row, column=1).value = utility
            ws.cell(row=row, column=2).value = total_poles
            ws.cell(row=row, column=3).value = completed
            ws.cell(row=row, column=4).value = remaining
            ws.cell(row=row, column=5).value = f"{completion_pct:.1f}%"
            ws.cell(row=row, column=6).value = est_completion
            
            row += 1
        
        logging.info("Completed Utility Progress section")

    def _generate_osp_productivity(self, ws):
        """Generate the OSP Productivity section."""
        logging.info("Generating OSP Productivity section")
        
        # Section header
        ws.cell(row=self.current_row, column=1).value = "OSP Productivity"
        ws.cell(row=self.current_row, column=1).font = Font(bold=True)
        
        # Column headers
        headers = ['User', 'Role', 'Completed Poles', 'Utilities', 'Last Activity']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=self.current_row + 1, column=col)
            cell.value = header
            cell.font = Font(bold=True)
        
        self.current_row += 2
        
        # Get weekly status metrics
        weekly_status = self.metrics.get_weekly_status()
        user_metrics = weekly_status.get('user_production', {})
        logging.info(f"Processing user productivity metrics: {user_metrics}")
        
        # Process field users
        logging.info("  Field Users:")
        field_users = user_metrics.get('field', [])
        for user_data in field_users:
            logging.debug(f"Processing field user: {user_data}")
            
            ws.cell(row=self.current_row, column=1).value = user_data.get('user', 'Unknown')
            ws.cell(row=self.current_row, column=2).value = 'Field'
            ws.cell(row=self.current_row, column=3).value = user_data.get('completed_poles', 0)
            ws.cell(row=self.current_row, column=4).value = ', '.join(user_data.get('utilities', []))
            ws.cell(row=self.current_row, column=5).value = max(user_data.get('dates', [])) if user_data.get('dates', []) else 'N/A'
            
            self.current_row += 1
            
        # Process back office users by category
        back_office_categories = ['annotation', 'sent_to_pe', 'delivery', 'emr', 'approved']
        for category in back_office_categories:
            logging.info(f"  {category.title()} Users:")
            category_users = user_metrics.get(category, [])
            for user_data in category_users:
                logging.debug(f"Processing {category} user: {user_data}")
                
                ws.cell(row=self.current_row, column=1).value = user_data.get('user', 'Unknown')
                ws.cell(row=self.current_row, column=2).value = category.replace('_', ' ').title()
                ws.cell(row=self.current_row, column=3).value = user_data.get('completed_poles', user_data.get('pole_count', 0))
                ws.cell(row=self.current_row, column=4).value = ', '.join(user_data.get('utilities', []))
                ws.cell(row=self.current_row, column=5).value = max(user_data.get('dates', [])) if user_data.get('dates', []) else 'N/A'
                
                self.current_row += 1
        
        logging.info("Completed OSP Productivity section")
        self.current_row += 2

    def _generate_charts(self, ws):
        """Generate the charts sheet."""
        # Utility Completion Rates Chart
        utility_chart = BarChart()
        utility_chart.title = "Utility Completion Rates"
        utility_chart.style = 10
        utility_chart.x_axis.title = "Utility"
        utility_chart.y_axis.title = "Rate"
        
        # Get data ranges from Weekly Metrics sheet
        data = Reference(self.wb['Weekly Status'], 
                        min_col=2, max_col=3,  # Current and Previous Rate columns
                        min_row=6,  # Header row
                        max_row=6 + len(self.metrics.burndown['by_utility']))
        cats = Reference(self.wb['Weekly Status'],
                        min_col=1, max_col=1,  # Utility column
                        min_row=7,  # Data start row
                        max_row=6 + len(self.metrics.burndown['by_utility']))
        
        utility_chart.add_data(data, titles_from_data=True)
        utility_chart.set_categories(cats)
        
        # OSP Productivity Chart
        osp_chart = BarChart()
        osp_chart.title = "OSP Productivity"
        osp_chart.style = 10
        osp_chart.x_axis.title = "Worker"
        osp_chart.y_axis.title = "Count"
        
        # Calculate OSP data range
        osp_start_row = 7 + len(self.metrics.burndown['by_utility']) + 3  # Header row for OSP section
        osp_end_row = osp_start_row + len(self.metrics.get_weekly_status()['user_production']['annotation'])
        
        data = Reference(self.wb['Weekly Status'],
                        min_col=2, max_col=3,  # Jobs and Poles columns
                        min_row=osp_start_row,
                        max_row=osp_end_row)
        cats = Reference(self.wb['Weekly Status'],
                        min_col=1, max_col=1,  # Worker column
                        min_row=osp_start_row + 1,
                        max_row=osp_end_row)
        
        osp_chart.add_data(data, titles_from_data=True)
        osp_chart.set_categories(cats)
        
        # Position charts side by side
        ws.add_chart(utility_chart, "B2")
        ws.add_chart(osp_chart, "J2")
        
    def _format_worksheet(self, ws):
        """Apply consistent formatting to the worksheet."""
        # Adjust column widths
        for col in ws.columns:
            max_length = 0
            column = col[0].column_letter
            
            for cell in col:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            
            adjusted_width = (max_length + 2)
            ws.column_dimensions[column].width = adjusted_width
        
        # Center align all cells
        for row in ws.rows:
            for cell in row:
                cell.alignment = Alignment(horizontal='center') 