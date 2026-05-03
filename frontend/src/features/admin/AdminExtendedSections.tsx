import { AdminAgentSection } from "@/features/admin/sections/AdminAgentSection";
import { AdminChunksSection } from "@/features/admin/sections/AdminChunksSection";
import { AdminChromaSection } from "@/features/admin/sections/AdminChromaSection";
import { AdminCorpusSection } from "@/features/admin/sections/AdminCorpusSection";
import { AdminKgSection } from "@/features/admin/sections/AdminKgSection";
import { AdminOperationsSection } from "@/features/admin/sections/AdminOperationsSection";
import type { AdminExtendedSectionProps } from "@/features/admin/sections/AdminExtendedSectionSupport";

export type { AdminExtendedSection } from "@/features/admin/adminModels";

export function AdminExtendedSections(props: AdminExtendedSectionProps) {
  switch (props.section) {
    case "corpus":
      return <AdminCorpusSection {...props} />;
    case "chunks":
      return <AdminChunksSection {...props} />;
    case "kg":
      return <AdminKgSection {...props} />;
    case "chroma":
      return <AdminChromaSection {...props} />;
    case "agent":
      return <AdminAgentSection {...props} />;
    default:
      return <AdminOperationsSection {...props} />;
  }
}
