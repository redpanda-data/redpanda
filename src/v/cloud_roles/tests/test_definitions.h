/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

namespace cloud_role_tests {
constexpr const char* gcp_oauth_token = R"json(
{"access_token":"a-token","expires_in":3599,"token_type":"Bearer"}
)json";

constexpr const char* gcp_url
  = "/computeMetadata/v1/instance/service-accounts/default/token";

constexpr const char* aws_role_query_url
  = "/latest/meta-data/iam/security-credentials/";

constexpr const char* aws_role = "tomato";

constexpr const char* aws_creds_url
  = "/latest/meta-data/iam/security-credentials/tomato";

constexpr const char* aws_creds = R"json(
{
  "Code" : "Success",
  "LastUpdated" : "2012-04-26T16:39:16Z",
  "Type" : "AWS-HMAC",
  "AccessKeyId" : "my-key",
  "SecretAccessKey" : "my-secret",
  "Token" : "my-token",
  "Expiration" : "2017-05-17T15:09:54.1234Z"
}
)json";

constexpr const char* sts_creds = R"xml(
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>sts-key</AccessKeyId>
      <SecretAccessKey>sts-secret</SecretAccessKey>
      <SessionToken>sts-token</SessionToken>
      <Expiration>2022-06-07T15:10:47.1234Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>
)xml";

constexpr const char* token_file = "token_here";

} // namespace cloud_role_tests
