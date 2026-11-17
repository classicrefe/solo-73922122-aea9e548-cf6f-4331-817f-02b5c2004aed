// Polls /api/status and renders the progress card.  A round from an older
// poll must never overwrite a newer one: each render is tagged with the
// response generation, and only the newest generation may touch the DOM.
(() => {
  const area = document.getElementById("run-area");
  const startBtn = document.getElementById("btn-start");
  const cancelBtn = document.getElementById("btn-cancel");
  const restartBtn = document.getElementById("btn-restart");
  const confirmBtn = document.getElementById("btn-confirm");
  const rejectBtn = document.getElementById("btn-reject");
  if (!area) return;

  let generation = 0;
  let busy = false;
  let seenActiveId = null;
  let seenPendingId = null;
  let initialised = false;

  const activeId = (status) => (status.active ? status.active.id : null);
  const pendingId = (status) =>
    status.pending_confirmation ? status.pending_confirmation.id : null;

  const esc = (value) =>
    String(value ?? "").replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[c]));

  function render(status) {
    const active = status.active;
    const pending = status.pending_confirmation;
    if (pending) {
      const withdrawals = status.pending_withdrawals || [];
      const listed = withdrawals
        .slice(0, 20)
        .map((item) => `<li class="mono">${esc(item.identifier)}</li>`)
        .join("");
      const more = withdrawals.length > 20
        ? `<li class="muted">…另有 ${withdrawals.length - 20} 条</li>` : "";
      area.innerHTML = `
        <div class="progress">
          <div class="row"><span>状态</span><strong class="badge awaiting_confirmation">待撤回确认</strong></div>
          <div class="row"><span>时间窗</span><span class="mono">${esc(pending.window_from || "全量")} .. ${esc(pending.window_until || "—")}</span></div>
          <div class="row"><span>将删除现存书目</span><strong>${pending.quarantine_deletions} / 目录 ${pending.quarantine_catalog_before} 条</strong></div>
          <div class="row"><span>待发布暂存</span><strong>${pending.staged_count ?? 0} 条</strong></div>
          <div class="row"><span>已发布水位</span><span class="mono">${esc(status.published.watermark || "（未采集）")}</span></div>
        </div>
        <p class="hint">目录与水位未改变，确认后才能发布并开始新轮。待撤回标识：</p>
        <ul class="runlist">${listed}${more}</ul>`;
      startBtn.disabled = true;
      cancelBtn.disabled = true;
      restartBtn.disabled = true;
      if (confirmBtn) confirmBtn.disabled = false;
      if (rejectBtn) rejectBtn.disabled = false;
      return;
    }
    if (!active) {
      area.innerHTML =
        '<p class="muted">当前没有运行中的轮次。已发布目录与水位保持上一次成功结果。</p>';
      startBtn.disabled = false;
      cancelBtn.disabled = true;
      restartBtn.disabled = false;
      if (confirmBtn) confirmBtn.disabled = true;
      if (rejectBtn) rejectBtn.disabled = true;
      return;
    }
    const pages = active.pages_fetched || 0;
    const total = active.complete_list_size;
    const pct = total ? Math.min(100, Math.round((active.records_seen / total) * 100)) : null;
    const attemptsLeft = Math.max(0, (status.config.max_attempts || 3) - (active.attempt || 1));
    area.innerHTML = `
      <div class="progress">
        <div class="bar"><i style="width:${pct === null ? 15 : pct}%"></i></div>
        <div class="row"><span>状态</span><strong class="badge ${esc(active.status)}">${esc(active.status)}</strong></div>
        <div class="row"><span>阶段</span><strong>${esc(active.phase || "—")}</strong></div>
        <div class="row"><span>时间窗</span><span class="mono">${esc(active.window_from || "全量")} .. ${esc(active.window_until || "待定（读取源端时刻）")}</span></div>
        <div class="row"><span>已取页数</span><strong>${pages}</strong></div>
        <div class="row"><span>已取记录</span><strong>${active.records_seen}${total ? " / " + total : ""}</strong></div>
        <div class="row"><span>分页作废重取</span><strong>${active.token_restarts || 0} 次</strong></div>
        <div class="row"><span>当前尝试</span><strong>${active.attempt || 1}（剩余重试 ${attemptsLeft}）</strong></div>
        <div class="row"><span>已暂存唯一标识</span><strong>${active.staged_count ?? 0}</strong></div>
        ${active.last_error ? `<div class="row"><span>最近错误</span><span>${esc(active.last_error)}</span></div>` : ""}
      </div>`;
    startBtn.disabled = true;
    cancelBtn.disabled = false;
    restartBtn.disabled = false;
    if (confirmBtn) confirmBtn.disabled = true;
    if (rejectBtn) rejectBtn.disabled = true;
  }

  function renderPublished(status, publishedEl, sizeEl) {
    if (publishedEl) publishedEl.textContent = status.published.watermark || "（尚未成功采集）";
    if (sizeEl) sizeEl.textContent = status.published.catalog_size;
  }

  async function poll() {
    const mine = ++generation;
    try {
      const response = await fetch("/api/status", { cache: "no-store" });
      if (!response.ok) throw new Error("status " + response.status);
      const status = await response.json();
      if (mine !== generation) return; // a newer poll already rendered
      if (!initialised) {
        initialised = true;
        seenActiveId = activeId(status);
        seenPendingId = pendingId(status);
      }
      if (
        activeId(status) !== seenActiveId ||
        pendingId(status) !== seenPendingId
      ) {
        // A round started/finished or entered/left withdrawal review: refresh
        // so the catalogue, watermark, review panel and history are accurate.
        seenActiveId = activeId(status);
        seenPendingId = pendingId(status);
        setTimeout(() => location.reload(), 400);
        return;
      }
      render(status);
      renderPublished(
        status,
        document.getElementById("watermark"),
        document.getElementById("catalog-size"),
      );
    } catch (error) {
      if (mine === generation) {
        area.innerHTML =
          '<p class="muted">无法读取状态：' + esc(error.message) + "</p>";
      }
    }
  }

  async function post(url) {
    if (busy) return;
    busy = true;
    startBtn.disabled = cancelBtn.disabled = restartBtn.disabled = true;
    if (confirmBtn) confirmBtn.disabled = true;
    if (rejectBtn) rejectBtn.disabled = true;
    try {
      const response = await fetch(url, { method: "POST" });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        alert(body.detail || "操作失败");
      } else if (body.committed === false && body.already_committed) {
        alert(body.reason || "本轮已发布");
      }
    } catch (error) {
      alert("请求失败：" + error.message);
    } finally {
      busy = false;
      await poll();
    }
  }

  startBtn?.addEventListener("click", () => post("/api/harvest"));
  cancelBtn?.addEventListener("click", () => {
    if (!confirm("取消当前轮次？本轮不会发布，已发布目录和水位保持不变。")) return;
    post("/api/harvest/cancel");
  });
  restartBtn?.addEventListener("click", () => post("/api/harvest/restart"));
  confirmBtn?.addEventListener("click", () => {
    if (!confirm("确认发布本轮？将删除列出的书目并原子更新目录和水位。")) return;
    post("/api/harvest/confirm");
  });
  rejectBtn?.addEventListener("click", () => {
    if (!confirm("驳回本轮？暂存将被清理，目录和水位保持原样。")) return;
    post("/api/harvest/reject");
  });

  poll();
  setInterval(poll, 1000);
})();
