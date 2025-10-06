'use strict';

const db = require('@arangodb').db;
const databaseName = 'project_graph';

if (!db._databases().includes(databaseName)) {
  print('creating new database: ' + databaseName);
  db._createDatabase(databaseName, { users: [{ username: "root", passwd: "arango2rdb" }] });
  print('done');
}

db._useDatabase(databaseName);
const database = require('@arangodb').db;

function ensureCollection(name) {
  let collection = database._collection(name);
  if (!collection) {
    collection = database._createDocumentCollection(name);
  }
  collection.truncate();
  return collection;
}

const teams = ensureCollection('teams');
teams.save({
  _key: 'team-dev',
  name: 'Development Team',
  leadMemberId: 'member-oliver'
});
teams.save({
  _key: 'team-design',
  name: 'Design Team',
  leadMemberId: 'member-ava'
});

const members = ensureCollection('members');
members.save({
  _key: 'member-oliver',
  firstName: 'Oliver',
  lastName: 'Mason',
  email: 'oliver.mason@example.com',
  role: 'Engineering Manager',
  teamId: 'team-dev'
});
members.save({
  _key: 'member-ava',
  firstName: 'Ava',
  lastName: 'Nguyen',
  email: 'ava.nguyen@example.com',
  role: 'Design Lead',
  teamId: 'team-design'
});
members.save({
  _key: 'member-liam',
  firstName: 'Liam',
  lastName: 'Garcia',
  email: 'liam.garcia@example.com',
  role: 'Backend Engineer',
  teamId: 'team-dev'
});
members.save({
  _key: 'member-sofia',
  firstName: 'Sofia',
  lastName: 'Khan',
  email: 'sofia.khan@example.com',
  role: 'Product Designer',
  teamId: 'team-design'
});

const projects = ensureCollection('projects');
projects.save({
  _key: 'project-analytics',
  name: 'Analytics Platform Refresh',
  description: 'Rebuild the analytics data pipeline and dashboards.',
  status: 'ACTIVE',
  teamId: 'team-dev',
  startDate: '2024-01-15',
  endDate: '2024-12-15'
});
projects.save({
  _key: 'project-mobile',
  name: 'Mobile App Redesign',
  description: 'Modernise the mobile application UX and UI with new onboarding flows.',
  status: 'PLANNING',
  teamId: 'team-design',
  startDate: '2024-03-01',
  endDate: '2024-09-30'
});


const projectHealth = ensureCollection('project_health');
projectHealth.save({
  _key: 'health-project-analytics',
  projectId: 'project-analytics',
  status: 'AT_RISK',
  updatedAt: '2024-04-10T10:00:00Z'
});
projectHealth.save({
  _key: 'health-project-mobile',
  projectId: 'project-mobile',
  status: 'ON_TRACK',
  updatedAt: '2024-03-25T15:30:00Z'
});

const tasks = ensureCollection('tasks');
tasks.save({
  _key: 'task-data-model',
  projectId: 'project-analytics',
  name: 'Define canonical data model',
  status: 'IN_PROGRESS',
  dueDate: '2024-04-30',
  assignedTeamId: 'team-dev'
});
tasks.save({
  _key: 'task-dashboard',
  projectId: 'project-analytics',
  name: 'Build executive dashboard',
  status: 'NOT_STARTED',
  dueDate: '2024-06-15',
  assignedTeamId: 'team-dev'
});
tasks.save({
  _key: 'task-onboarding-flow',
  projectId: 'project-mobile',
  name: 'Prototype onboarding flow',
  status: 'IN_REVIEW',
  dueDate: '2024-05-20',
  assignedTeamId: 'team-design'
});

tasks.save({
  _key: 'task-style-guide',
  projectId: 'project-mobile',
  name: 'Update mobile design system',
  status: 'IN_PROGRESS',
  dueDate: '2024-06-10',
  assignedTeamId: 'team-design'
});

const assignments = ensureCollection('task_assignments');
assignments.save({
  _key: 'assign-data-model-liam',
  taskId: 'task-data-model',
  memberId: 'member-liam',
  hoursPlanned: 40,
  hoursActual: 12
});
assignments.save({
  _key: 'assign-dashboard-oliver',
  taskId: 'task-dashboard',
  memberId: 'member-oliver',
  hoursPlanned: 24,
  hoursActual: 0
});
assignments.save({
  _key: 'assign-onboarding-ava',
  taskId: 'task-onboarding-flow',
  memberId: 'member-ava',
  hoursPlanned: 32,
  hoursActual: 18
});
assignments.save({
  _key: 'assign-style-sofia',
  taskId: 'task-style-guide',
  memberId: 'member-sofia',
  hoursPlanned: 28,
  hoursActual: 9
});

const milestones = ensureCollection('milestones');
milestones.save({
  _key: 'milestone-analytics-alpha',
  projectId: 'project-analytics',
  name: 'Analytics alpha release',
  targetDate: '2024-08-01',
  status: 'PLANNED'
});
milestones.save({
  _key: 'milestone-mobile-beta',
  projectId: 'project-mobile',
  name: 'Mobile beta handoff',
  targetDate: '2024-07-15',
  status: 'PLANNED'
});

print('Sample project graph data loaded.');
