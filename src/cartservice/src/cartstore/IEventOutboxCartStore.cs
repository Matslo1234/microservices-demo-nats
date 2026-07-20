// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Threading.Tasks;

namespace cartservice.cartstore
{
    public interface IEventOutboxCartStore : ICartStore
    {
        Task AddItemWithEventAsync(string userId, string productId, int quantity,
            string requestId, string traceparent, string tracestate);
        Task EmptyCartWithEventAsync(string userId, string requestId,
            string traceparent, string tracestate);
    }
}
