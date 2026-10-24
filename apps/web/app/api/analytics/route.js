import { getAnalytics } from "../../../lib/data.js";
import { respond } from "../../../lib/api.js";
export const dynamic = "force-dynamic";
export async function GET(request) { return respond(() => getAnalytics(Object.fromEntries(new URL(request.url).searchParams))); }
