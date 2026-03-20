import { useState } from "react";
import {
  ResizablePanelGroup,
  ResizablePanel,
  ResizableHandle,
} from "@/components/ui/resizable";
import { Navbar } from "@/components/layout/Navbar";
import { LeftPanel } from "@/components/layout/LeftPanel";
import { BottomPanel } from "@/components/results/BottomPanel";
import { RightPanel } from "@/components/inspector/RightPanel";
import { ForceGraph } from "@/components/graph/ForceGraph";
import { FullscreenExplorer } from "@/components/graph/FullscreenExplorer";
import { useUiStore } from "@/stores/uiStore";

export function AppShell() {
  const rightPanelOpen = useUiStore((s) => s.rightPanelOpen);
  const bottomPanelOpen = useUiStore((s) => s.bottomPanelOpen);
  const [fullscreen, setFullscreen] = useState(false);

  return (
    <div className="flex h-screen flex-col bg-background text-foreground">
      <Navbar />

      <ResizablePanelGroup orientation="horizontal" className="flex-1">
        <ResizablePanel defaultSize="20%" minSize="180px">
          <LeftPanel />
        </ResizablePanel>

        <ResizableHandle withHandle />

        <ResizablePanel minSize="300px">
          {bottomPanelOpen ? (
            <ResizablePanelGroup orientation="vertical">
              <ResizablePanel defaultSize="60%" minSize="150px">
                <ForceGraph onFullscreen={() => setFullscreen(true)} />
              </ResizablePanel>

              <ResizableHandle withHandle />

              <ResizablePanel defaultSize="40%" minSize="100px">
                <BottomPanel />
              </ResizablePanel>
            </ResizablePanelGroup>
          ) : (
            <ForceGraph onFullscreen={() => setFullscreen(true)} />
          )}
        </ResizablePanel>

        {rightPanelOpen && (
          <>
            <ResizableHandle withHandle />

            <ResizablePanel defaultSize="20%" minSize="200px">
              <RightPanel />
            </ResizablePanel>
          </>
        )}
      </ResizablePanelGroup>

      {/* Fullscreen exploration overlay */}
      <FullscreenExplorer open={fullscreen} onClose={() => setFullscreen(false)} />
    </div>
  );
}
