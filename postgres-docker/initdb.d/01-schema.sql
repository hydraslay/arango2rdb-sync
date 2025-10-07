CREATE TABLE IF NOT EXISTS teams (
    team_id VARCHAR(64) PRIMARY KEY,
    name TEXT NOT NULL,
    lead_member_id VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS members (
    member_id VARCHAR(64) PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    role TEXT NOT NULL,
    team_id VARCHAR(64) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS projects (
    project_id VARCHAR(64) PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    team_id VARCHAR(64) REFERENCES teams(team_id),
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR(64) PRIMARY KEY,
    project_id VARCHAR(64) REFERENCES projects(project_id),
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    due_date DATE,
    assigned_team_id VARCHAR(64) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS task_assignments (
    assignment_id VARCHAR(64) PRIMARY KEY,
    task_id VARCHAR(64) REFERENCES tasks(task_id),
    member_id VARCHAR(64) REFERENCES members(member_id),
    hours_planned INTEGER,
    hours_actual INTEGER
);

CREATE TABLE IF NOT EXISTS milestones (
    milestone_id VARCHAR(64) PRIMARY KEY,
    project_id VARCHAR(64) REFERENCES projects(project_id),
    name TEXT NOT NULL,
    target_date DATE,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_overview (
    project_id VARCHAR(64) PRIMARY KEY,
    project_name TEXT NOT NULL,
    team_name TEXT NOT NULL,
    health_status TEXT,
    health_updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project_observations(
    project_id VARCHAR(64) PRIMARY KEY,
    project_name TEXT NOT NULL,
    observer_name TEXT
);

