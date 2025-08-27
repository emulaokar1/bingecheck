import { ShowPage } from "../../../components/ShowPage";

interface ShowDetailPageProps {
  params: Promise<{ id: string }>;
}

export default async function ShowDetailPage({ params }: ShowDetailPageProps) {
  const { id } = await params;
  const showId = parseInt(id, 10);
  
  return <ShowPage showId={showId} />;
}