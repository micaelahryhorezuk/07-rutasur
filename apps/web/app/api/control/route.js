import { getControl } from "../../../lib/data.js";
import { respond } from "../../../lib/api.js";
export const dynamic = "force-dynamic";
export async function GET(request) { return respond(() => getControl(Object.fromEntries(new URL(request.url).searchParams))); }
