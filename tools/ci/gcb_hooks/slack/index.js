const {IncomingWebhook} = require('@slack/webhook');
const url               = process.env.SLACK_WEBHOOK_URL;

const webhook = new IncomingWebhook(url);

// subscribeSlack is the main function called by Cloud Functions.
module.exports.subscribeSlack = (pubSubEvent, context) => {
  const build = eventToBuild(pubSubEvent.data);

  // Skip if manually triggered
  if (!build.hasOwnProperty('buildTriggerId')) {
    return;
  }

  // Skip if the current status is not in the status list.
  // Add additional statuses to list if you'd like:
  // QUEUED, WORKING, SUCCESS, FAILURE,
  // INTERNAL_ERROR, TIMEOUT, CANCELLED
  const status = ['SUCCESS', 'FAILURE', 'INTERNAL_ERROR', 'TIMEOUT'];
  if (status.indexOf(build.status) === -1) {
    return;
  }

  // Send message to Slack.
  const message = createSlackMessage(build);
  webhook.send(message);
};

// eventToBuild transforms pubsub event message to a build object.
const eventToBuild =
  (data) => { return JSON.parse(Buffer.from(data, 'base64').toString()); };

// createSlackMessage creates a message from a build object.
const createSlackMessage = (build) => {
  console.log(JSON.stringify(build));
  let repo, branch, sha;
  if (build.source && build.source.repoSource) {
    ({repo, branch} = build.source.repoSource);
    sha = build.sourceProvenance.resolvedRepoSource.commitSha.substring(0, 8);
  } else if (build.substitutions) {
    repo   = build.substitutions.REPO_NAME;
    branch = build.substitutions.BRANCH_NAME;
    sha    = build.substitutions.SHORT_SHA;
  }
  const type = build.substitutions._BUILD_TYPE;
  const cc   = build.substitutions._COMPILER;
  // TODO: Add "thread_ts": sha,
  const message = {
    'blocks': [{
      'type': 'section',
      'text': {
        'type': 'mrkdwn',
        'text': `${build.status} <${build.logUrl} | ${repo}/${branch}@${sha}-${
          cc}-${type}>`
      }
    }]
  };

  return message;
};
