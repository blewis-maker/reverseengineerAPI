# Status Change and Project Tracking Implementation Plan

## 1. Database Schema Updates

### Project Tracking Tables
- [ ] Create `projects` table with fields:
  - project_id (Zone Name)
  - name
  - utility
  - total_poles
  - target_date (Aerial Engineering due date)
  - status
  - created_at
  - updated_at

### Burndown Tracking Tables
- [x] Create `burndown_metrics` table with fields:
  - id
  - entity_type (utility/project)
  - entity_id
  - total_poles
  - field_complete
  - back_office_complete
  - run_rate
  - estimated_completion
  - timestamp
  Implementation details:
  - Added unique constraint on (utility, date)
  - Added calculation for run_rate based on daily completion rate
  - Added resource tracking (actual vs required)
  - Implemented backfill functionality

## 2. SharePoint Integration

### Design Job Tracking Import
- [ ] Create function to fetch Design Job Tracking spreadsheet
- [ ] Parse Zone Names and due dates
- [ ] Map to existing projects in database
- [ ] Update project target dates

## 3. Burndown Calculations

### Master Burndown
- [x] Calculate total poles across all projects
- [x] Track completion percentage
- [x] Calculate overall run rate
- [x] Project completion date
Implementation details:
- Using daily snapshots for point-in-time analysis
- Calculating run rate based on completed poles over time
- Estimating completion dates using current run rate

### Field Burndown
- [x] Track field completion status
- [x] Calculate field run rate
- [x] Project field completion date
Implementation details:
- Tracking field_completed status in pole_metrics
- Calculating field-specific run rates
- Recording field resources and completion timestamps

### Back Office Burndown
- [x] Track back office completion status
- [x] Calculate back office run rate
- [x] Project back office completion date
Implementation details:
- Tracking back_office_completed status in pole_metrics
- Calculating back office run rates
- Recording annotation resources and completion timestamps

## 4. Project Integration

### Job to Project Mapping
- [ ] Map jobs to projects using Zone Name
- [ ] Aggregate job metrics by project
- [ ] Calculate project-level statistics

### Project Schedule Tracking
- [ ] Track project due dates from Design Job Tracking
- [ ] Calculate project completion estimates
- [ ] Compare with target dates
- [ ] Status indicators (On Track/At Risk/Behind)

## 5. Weekly Report Enhancements

### Project Status Section
- [ ] Add project summary table
- [ ] Show completion by project
- [ ] Compare actual vs target dates
- [ ] Resource allocation by project

### Enhanced Burndown Charts
- [x] Master burndown chart
- [x] Field completion burndown
- [x] Back office completion burndown
- [ ] Project-specific burndowns
Implementation details:
- Using burndown_metrics table for trend analysis
- Calculating separate field and back office completion rates
- Tracking resource allocation and requirements

## 6. Testing Plan

### Database Testing
- [x] Test project table creation
- [x] Test burndown metrics recording
- [x] Verify data integrity
Implementation details:
- Added unique constraints to prevent duplicates
- Implemented comprehensive schema with proper relationships
- Added indexes for query performance
- Verified data integrity through verify_tables.py

### SharePoint Integration Testing
- [ ] Test Design Job Tracking import
- [ ] Verify project mapping
- [ ] Validate target date updates

### Burndown Calculation Testing
- [x] Verify master burndown accuracy
- [x] Test field burndown calculations
- [x] Test back office burndown calculations
- [x] Validate run rate calculations
Implementation details:
- Implemented backfill functionality for historical data
- Added error handling for division by zero cases
- Verified calculations through metrics_recorder.py

### Project Integration Testing
- [ ] Test job-to-project mapping
- [ ] Verify project metrics aggregation
- [ ] Test schedule tracking
- [ ] Validate status indicators

### Report Generation Testing
- [x] Test enhanced report format
- [x] Verify burndown charts
- [ ] Validate project status section
- [ ] Test overall report generation

## Implementation Strategy

1. **Phase 1: Database Setup** ✓
   - Implement schema changes
   - Create new tables
   - Set up relationships

2. **Phase 2: Data Integration** (In Progress)
   - Implement SharePoint integration
   - Set up project mapping
   - Begin collecting burndown metrics

3. **Phase 3: Calculation Engine** ✓
   - Implement burndown calculations
   - Set up run rate tracking
   - Create completion projections

4. **Phase 4: Reporting** (Partially Complete)
   - Enhance weekly report
   - Add new visualizations
   - Implement project tracking

5. **Phase 5: Testing & Validation** (In Progress)
   - Systematic testing of each component
   - End-to-end testing
   - Performance optimization

## Notes

- Project (Zone Name) is the key linking entity
- Burndowns are now calculated at multiple levels:
  - Master (all poles)
  - Field completion
  - Back office completion
  - Project-specific (pending)
- Schedule tracking will integrate Design Job Tracking data
- Weekly report includes both high-level and detailed views
- Added comprehensive error handling and data validation
- Implemented backfill functionality for historical data
