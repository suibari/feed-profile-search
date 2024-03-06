import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import dotenv from 'dotenv'
import { BskyAgent } from '@atproto/api'
import { QueryParams as QueryParamsAuthors } from '@atproto/api/dist/client/types/app/bsky/unspecced/searchActorsSkeleton'
import { OutputSchema, QueryParams as QueryParamsFeeds } from './lexicon/types/app/bsky/feed/getAuthorFeed'
import { Database } from './db'
import { ProfileView } from '@atproto/api/dist/client/types/app/bsky/actor/defs'
import { appendFile } from 'fs'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    for (const post of ops.posts.creates) {
      console.log(post.record.text)
    }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only alf-related posts
        return create.record.text.toLowerCase().includes('alf')
      })
      .map((create) => {
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}

export class ScpecificActorsSubscription {
  agent:BskyAgent

  constructor(public db: Database) {
    this.agent = new BskyAgent({
      service: 'https://bsky.social'
    })
  }

  async run() {
    await this.reload()
  }

  async reload() {
    let rowcount = 0;

    // Bearer取得
    dotenv.config()
    await this.agent.login({
      identifier: process.env.FEEDGEN_PUBLISHER_IDENTIFIER || '',
      password: process.env.FEEDGEN_APP_PASSWORD || ''
    })

    // プロフィール検索
    // searchActorsがOR検索に対応してないので、QUERYを分割検索し最後に結果を結合
    let actors_arr: ProfileView[] = []
    const q_arr = process.env.FEEDGEN_QUERY?.split(' ')
    if (!q_arr) {
      console.error("query parameter is null!")
      return
    }
    for (let q of q_arr) {
      const params_actors:QueryParamsAuthors = {
        q: q,
        limit: 100
      }
      let cursor: string | undefined = undefined;
      do {
        const { data: data_actors } = await this.agent.searchActors({...params_actors, cursor});
        actors_arr = actors_arr.concat(...data_actors.actors);
        cursor = data_actors.cursor;
      } while (cursor);
    }
    console.log(actors_arr.length);

    // 検索して見つかった全ユーザに対して実行
    for (let actor of actors_arr) {
      // console.log(actor.description)
      // ポスト取得
      const params_feed:QueryParamsFeeds = {
        actor: actor.did,
        limit: 100,
        filter: 'posts_no_replies'
      }
      const { data: data_feed } = await this.agent.getAuthorFeed(params_feed)
      const { feed: postsArray, cursor: nextPage } = data_feed
      for (let post of postsArray) {
        if (!post.reason) {
          // console.log(post.post.record)
          // DB格納
          const postsToCreate = {
            uri: post.post.uri,
            cid: post.post.cid,
            indexedAt: post.post.indexedAt
          }
          await this.db
            .insertInto('post')
            .values(postsToCreate)
            .onConflict(oc => oc.doNothing())
            .execute()
          rowcount++
        }
      }
    }
    console.log("[INFO] inserted total " + rowcount + " post to DB.");
  }

  intervalId = setInterval(async () => {
    await this.reload()
  }, 10*60*1000) // 10m
}
