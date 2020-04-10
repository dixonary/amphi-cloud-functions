import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import * as https from 'https';

admin.initializeApp();

import * as Querystring from 'querystring';

import fetch from 'node-fetch';
import * as FormData from 'form-data';

import {google} from 'googleapis';
import { DataSnapshot } from 'firebase-functions/lib/providers/database';
import { CallableContext } from 'firebase-functions/lib/providers/https';
import { v4 as uuidv4 } from 'uuid';

/******************************************************************************/
// Constants

const FRONTEND_URL       = 'https://amphi-compsoc.web.app';

const UWCS_URI_TOKEN     = 'https://uwcs.co.uk/o/token/';
const UWCS_URI_AUTHORIZE = 'https://uwcs.co.uk/o/authorize/';
const UWCS_URI_PROFILE   = `https://uwcs.co.uk/api/me`;
const UWCS_SCOPES        = ['lanapp'];

const AMPHI_BACKEND_TIMER_ENDPOINT = 'https://amphi.dixonary.co.uk';
const AMPHI_BACKEND_TIMER_TOKEN    = functions.config().amphi.token;
const AMPHI_BACKEND_TIMER_HOOK     = 'https://us-central1-amphi-compsoc.cloudfunctions.net/nextVideoCallback';

const UWCS_CLIENT_ID     = functions.config().uwcs.id;
const UWCS_CLIENT_SECRET = functions.config().uwcs.secret;
const GOOGLE_API_KEY     = functions.config().gapi.key;

const youtube = google.youtube({
  auth: GOOGLE_API_KEY,
  version: "v3"
});

/******************************************************************************/
// Redirects the User to the uwcs consent screen.

exports.uwcsAuth = functions.https.onRequest((req, res) => {

  const from = (req.query.host ?? FRONTEND_URL);
  const redirect_uri = from + "/auth/login";

  res.set('Access-Control-Allow-Origin' , from)
     .set('Access-Control-Allow-Methods', 'GET, POST')
     .set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');

  if (req.method === `OPTIONS`) { // Preflight Request
    res.sendStatus(200);
    return;
  }

  const params = Querystring.stringify({
    'grant_type'     : 'authorization_code',
    'response_type'  : 'code',
    'redirect_uri'   : redirect_uri,
    'client_id'      : UWCS_CLIENT_ID,
    'scope'          : UWCS_SCOPES
  });

  res.redirect(UWCS_URI_AUTHORIZE + "?" + params);

}); 



/******************************************************************************/
// Callback to finish authorizing with OAuth v2.
exports.uwcsAuthCallback = functions.https.onRequest(async (req, res) =>  {

  // Grab the code from the request parameters
  const code = req.query.code;
  const from = (req.query.host ?? FRONTEND_URL);
  const redirect_uri = from + "/auth/login";

  res.set('Access-Control-Allow-Origin', from)
     .set('Access-Control-Allow-Methods', 'GET, POST')
     .set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');

  if (req.method === `OPTIONS`) { // Preflight Request
    res.sendStatus(200);
    return;
  }

  // Make a request to the oauth server for a new key
  const form = new FormData();
  form.append('grant_type'   ,'authorization_code');
  form.append('code'         , code);
  form.append('redirect_uri' , redirect_uri);
  form.append('client_id'    , UWCS_CLIENT_ID);
  form.append('client_secret', UWCS_CLIENT_SECRET);

  await fetch(UWCS_URI_TOKEN, {
    method:'post', 
    headers: form.getHeaders(),
    body:form
  })
  .then((resp) => resp.json())
  .then(async (data) => {

    const accessToken = data.access_token;
    const refreshToken = data.refresh_token;

    await fetch(UWCS_URI_PROFILE, 
      {
        method:'GET',
        headers: [["Authorization",`Bearer ${accessToken}`]]
      }
    )
    .then((userResp) => userResp.json())
    .then(async (udata) => {

      const firebaseToken = await createFirebaseAccount(
        udata.user.username,  // Persistent uid
        udata.nickname,       // Current display name
        accessToken,
        refreshToken);

      res.jsonp({token:firebaseToken});

    });
  });

});

async function createFirebaseAccount(
        uwcsId:string, 
        nickname:string, 
        accessToken:string, 
        refreshToken:string) {

  // The UID we'll assign to the user.
  const uid = `uwcs:${uwcsId}`;

  // Create or update the user account.
  const createUserAccount = admin.auth().updateUser(uid, {displayName:nickname})
  .catch((error:any) => {
    // If user does not exists we create it.
    if (error.code === 'auth/user-not-found') {
      return admin.auth().createUser({
        uid,
        displayName: nickname
      });
    }
    throw error;
  });

  // Store the user's oauth-grabbed info in the database.
  const storeUserInDatabase = admin.database()
    .ref(`users/${uid}`)
    .update({
      uid,
      displayName:nickname
    });

  // Save the access token to the Firebase Realtime Database.
  const storeAccessToken = admin.database()
    .ref(`tokens/${uid}`)
    .set({"access_token":accessToken, "refresh_token":refreshToken});

  await Promise.all([createUserAccount, storeAccessToken, storeUserInDatabase]);

  // Todo: replace with the exec value from the OAuth scope
  const admins = [
    "uwcs:1300831", //dixonary
    "uwcs:1833194", //john
    "uwcs:1605235", //thebruce
    "uwcs:1618643", //distributive_law
   ];
  const isAdmin = admins.indexOf(uid) !== -1;
  if(isAdmin) 
    await admin.database().ref(`users/${uid}/isAdmin`).set(true);

  return await admin.auth().createCustomToken(uid, {isAdmin});

}


/******************************************************************************/
/* The timer backend will call this once a timer ends. */

exports.nextVideoCallback = functions.https.onRequest(async (req,res) => {

  const currentVideo = await admin.database().ref('currentVideo').once('value');

  if(currentVideo.val() === null) return;

  // Check that the nonce is correct
  // As there is no other way to get the nonce than by having been told it,
  // this is sufficient verification.
  if(req.body.nonce !== currentVideo.val().nonce) {
    res.sendStatus(409);
    return;
  }

  await nextVideo();
  res.sendStatus(200);
});


/******************************************************************************/
/* Get video information from the Youtube API in response to a new video. */

// TODO convert to realtime database

exports.newVideoInfo = functions.database.ref("videos/{videoId}")
  .onCreate((snapshot) => getVideoInfo(snapshot));
  

async function getVideoInfo (snapshot:DataSnapshot) {

    // Note: data in the snapshot should be exactly {loading: true}
    const res = await youtube.videos.list(
      {
        part:'contentDetails,snippet',
        maxResults: 1,
        id: snapshot.key
      }
    );

    if(res.data.items === undefined) return;
    const item = res.data.items[0];
    if(item === undefined) return;

    const duration     = item.contentDetails?.duration;
    const title        = item.snippet?.title;
    const channelTitle = item.snippet?.channelTitle;
    const thumbnail    = item.snippet?.thumbnails?.default?.url;

    await snapshot.ref.set({
      loading: false,
      duration,
      title,
      channelTitle,
      thumbnail
    });

};


/******************************************************************************/
/* Add "uploadedAt" metadata to a queue entry. */

exports.onEnqueue = functions.database
  .ref('queues/{uid}/{idx}')
  .onCreate((snapshot, context) => addMetadataAndUpdateGlobalPlaylist(snapshot, context));

async function addMetadataAndUpdateGlobalPlaylist(snapshot:DataSnapshot, context:functions.EventContext) {

  const videoId = snapshot.val()?.video;
  if(videoId === null || videoId === undefined) return;

  // Disallow if the video is on the blacklist
  const blacklistRef = admin.database().ref(`blacklist/${videoId}`);
  const blacklisted = (await blacklistRef.once('value')).val();

  if(blacklisted) {
    await snapshot.ref.remove();
    return;
  }

  // Disallow if a video was played too recently
  const now = Date.now();
  const minTimeDiffRef = admin.database().ref(`settings/minTimeDiff`);
  const minTimeDiff = (await minTimeDiffRef.once('value')).val();
  const lastPlayedRef = admin.database().ref(`history/${videoId}`);
  const then = await (await lastPlayedRef.once('value')).val()?.playedAt;

  if(then !== null && then !== undefined && now - then < minTimeDiff * 1000) {
    await snapshot.ref.remove();
    return;
  }

  // Disallow if a song is already enqueued
  const allQueuedRef = admin.database().ref(`allQueued`);
  const allQueued = (await allQueuedRef.once('value')).val();
  if(allQueued !== null && allQueued.indexOf(videoId) !== -1) {
    await snapshot.ref.remove();
    return;
  }

  // Disallow if user is banned
  const uid = context.params.uid;
  const bannedRef = admin.database().ref(`users/${uid}/status`);
  if((await bannedRef.once('value')).exists()) {
    await snapshot.ref.remove();
    return;
  }

  // Set the queued-time to now and add the song to the main playlist.
  await snapshot.ref.child('queuedAt').set(now);
  await updateGlobalPlaylist();
}


/******************************************************************************/
/* Check if we need to update global when someone's local playlist changes. */

exports.onQueueChange = functions.database
  .ref('queues/{uid}')
  .onUpdate((change) => checkForChanges(change));

async function checkForChanges(change:functions.Change<DataSnapshot>) {
  const queuePre  = change.before.val() as any[];
  const queuePost = change.after.val() as any[];

  // Prevent a re-firing on enqueue.
  if(queuePre !== null && queuePost !== null 
    && queuePost.length === queuePre.length + 1) {
    return;
  }

  // Precondition: Check if the queue has been modified
  let noChanges = true;
  // Creation or deletion
  if((queuePre === null) !== (queuePost === null)) {
    noChanges = false;
  }

  if(noChanges) {
    // Check if the lists are the same
    for(let idx=0; idx < Math.max(queuePre.length, queuePost.length); idx++) {
      if(queuePre[idx]?.video !== queuePost[idx]?.video) {
        noChanges = false;
        break;
      } 
    }
  }

  if(noChanges) return;

  await updateGlobalPlaylist();

}

exports.onQueueRemove = functions.database
  .ref('queues/{uid}')
  .onDelete((change) => updateGlobalPlaylist());


/******************************************************************************/
/* Update the global playlist. */

async function updateGlobalPlaylist() {

  const queuesRef  = admin.database().ref('queues');
  const bucketsRef = admin.database().ref('buckets');
  const playedRef  = admin.database().ref('played');
  const usersRef   = admin.database().ref('users');

  // Kinda sucks but also kinda unavoidable?
  const users = (await usersRef.once('value')).val();

  // List of UIDs which are blacklisted from the first bucket,
  // because their videos have been played already.
  const alreadyPlayed = (((await playedRef.once('value')).val()) as string[] | null)??[];


  const queues     = (await queuesRef.once('value')); 

  // Storage for new buckets to be generated.
  const allBuckets:object[][] = [];

  // Keep track of ALL songs we have queued.
  let totalSongs = 0;
  const allQueued = [] as string[];

  queues.forEach((snapshot) => {
    const uQueue = snapshot.val();

    if(uQueue === null) return;

    let idx = 0;
    uQueue.forEach(({video, queuedAt}:{video:string, queuedAt:Date}) => {
      totalSongs++;

      // We shift the buckets on by one if the person has already been in the 
      // first bucket.
      const played = alreadyPlayed.indexOf(snapshot.key??"") !== -1;

      const relIdx = played ? idx+1 : idx;

      if(allBuckets[relIdx] === undefined) {
        allBuckets[relIdx] = [];
      }

      allBuckets[relIdx].push({
        video, 
        queuedAt,
        queuedBy:snapshot.key,
        queuedByDisplayName:users[snapshot.key??""]?.displayName ?? "<unknown>"
      });

      allQueued.push(video ?? "");

      idx++;
    });
  });

  // Remove the first bucket if there are no videos in it!
  if(allBuckets[0] === undefined) {
    allBuckets.shift();
    await playedRef.remove();
  }

  const allBucketsObject:any = {};
  for(let idx=0; idx < allBuckets.length; idx++) {

    allBuckets[idx].sort((a:any,b:any) => {
      return a.queuedAt - b.queuedAt;
    });

    const currentBucketObject:any = {};
    let elem = 0;
    allBuckets[idx].forEach((v) => {currentBucketObject[elem++] = v;});

    allBucketsObject[idx.toString()] = currentBucketObject;
  }

  await admin.database().ref('allQueued').set(allQueued);
  await bucketsRef.set(allBucketsObject);

  const currentVideo = await admin.database().ref('currentVideo').once('value');
  if(totalSongs > 0 && currentVideo.val() === null) {
    await nextVideo();
  }

}


/******************************************************************************/
/* Dequeue someone's video. */
exports.admin_dequeueVideo = functions.https.onCall(
  async ({vidId, uid}:any, context: CallableContext) => {

    const isAdmin = await checkAdmin(context);
    if(isAdmin) {
      await dequeueVideo(vidId, uid);
    }

    return "OK";
  }
);

const dequeueVideo = async (vidId:string, uid:string) => {
  
  const queueRef = admin.database().ref(`queues/${uid}`);
  await queueRef.transaction((current:any) => {
    const q = (current as any[] | null) ?? [];

    const index = q.findIndex((entry) => entry.video === vidId);
    if(index === -1) return q;

    q.splice(index, 1);
    return current;
  })
}

const checkAdmin = async (context:CallableContext) => {
  if(context.auth === undefined) return false;
  const uid = context.auth.uid;

  const currentUserRef = admin.database().ref(`users/${uid}`);
  const currentUser = (await currentUserRef.once('value')).val();

  return currentUser.isAdmin === true;
}


/******************************************************************************/
/* Stop a video (if possible) and start the next one (if possible). */
exports.admin_playNextVideo = functions.https.onCall(
  async (data:any, context:CallableContext) => {
    // The queuer can also cancel the current video
    // although this is not currently supported on the frontend.
    const currentVidRef = admin.database().ref('currentVideo');
    const currentVideo = (await currentVidRef.once('value')).val();

    if(context.auth?.uid === currentVideo.queuedBy
      || (await checkAdmin(context))) {
      await nextVideo();
      return;
    }
  }
);

type VidInfo = {
  video:string, 
  queuedAt:Date, 
  queuedBy:string
};

async function nextVideo() {
  const currentVidRef = admin.database().ref('currentVideo');
  const playedRef  = admin.database().ref('played');

  const firstBucketRef = admin.database().ref('buckets/0');
  const firstBucket = (await firstBucketRef.once('value')).val() as VidInfo[] | null;

  const lastPlayedRef = admin.database().ref('history');

  // If there is no next video, just unset the currently playing video.
  if(firstBucket === null || firstBucket.length === 0) {
    await currentVidRef.remove();
    return;
  }

  const firstVideo     = firstBucket[0];
  const firstVideoData = await admin.database().ref(`videos/${firstVideo.video}`).once('value');


  const secondsDuration = await boundDuration(
    getDurationInSeconds(firstVideoData.val().duration)
  );

  const nonce = uuidv4();

  // Create the callback to end the song when done.
  const sendTimer = async () => {

    const form = new FormData();
    form.append('nonce'   , nonce);
    form.append('hook'    , AMPHI_BACKEND_TIMER_HOOK);
    form.append('token'   , AMPHI_BACKEND_TIMER_TOKEN);
    form.append('duration', secondsDuration);
  
    await fetch(AMPHI_BACKEND_TIMER_ENDPOINT, {
      method:'post', 
      headers: form.getHeaders(),
      body:form,
      agent:new https.Agent({
        rejectUnauthorized:false
      })
    });
  };

  // Update the currentVid.
  const updateCurrentVideo = currentVidRef.update({
    ...firstVideo,
    startedAt: Date.now(),
    seconds: secondsDuration,
    nonce,
  }).then(sendTimer);

  // Add the queueing user to the played list.
  const addPlayed = playedRef.transaction((played) => {
    const p = played ?? [];
    p.push(firstVideo.queuedBy)
    return p;
  });

  // Add the video to the list of recently played videos.
  const addTolastPlayedList = lastPlayedRef.transaction((lastPlayed) => {
    const l = lastPlayed ?? {};
    l[firstVideo.video] = {
      ...firstVideo,
      playedAt: Date.now()
    };
    return l;
  });

  // Remove the song from the relevant user's queue.
  const removeFromUserQueue = removeFirstVid(firstVideo.queuedBy);

  await Promise.all([
    updateCurrentVideo, 
    addPlayed, 
    removeFromUserQueue,
    addTolastPlayedList
  ]);
}


const boundDuration = async (rawDuration:number) => {
  const maxDuration = (await admin.database().ref(`settings/maxPlayTime`).once('value')).val() as number;
  if(maxDuration === null || maxDuration === 0) {
    return rawDuration;
  }
  return Math.min(rawDuration, maxDuration);
}

/******************************************************************************/
/* Convert an ISO-standard duration to a regular duration. */
function getDurationInSeconds(isoDuration:string):number {

  const DURATION_REGEX = /^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/;
  const dur_match = isoDuration.match(DURATION_REGEX);
  if(dur_match === null) return 60;

  const hours:number = parseInt(dur_match[1] ?? "0");
  const mins:number  = parseInt(dur_match[2] ?? "0");
  const secs:number  = parseInt(dur_match[3] ?? "0");
  return (hours * 3600 + mins*60 + secs);
};


/******************************************************************************/
/* Remove the first video from someone's queue.                               */
async function removeFirstVid(uid:string) {
  const queueRef = admin.database().ref(`queues/${uid}`);
  await queueRef.transaction((queuedItems:any[]) => {
    if(queuedItems === null) return null;
    queuedItems.shift();
    return queuedItems;
  });
}